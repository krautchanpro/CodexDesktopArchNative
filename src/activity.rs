use std::{
    collections::{HashMap, HashSet},
    env,
    fs::{self, File},
    io::{self, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::mpsc::{self, Receiver},
    thread,
    time::{Duration, Instant},
};

use memchr::memmem;

const TASK_STARTED_MARKER: &[u8] = br#""type":"task_started""#;
const TASK_COMPLETE_MARKER: &[u8] = br#""type":"task_complete""#;
const READ_CHUNK_BYTES: usize = 256 * 1024;
const MARKER_OVERLAP_BYTES: usize = 64;
const APP_SERVER_REFRESH_TICKS: u8 = 5;
const PROJECTION_RETRY_DELAY: Duration = Duration::from_secs(5);

/// Watches only task lifecycle markers in rollout files currently held open by
/// a local Codex app-server. This supplements the native client's own streamed
/// notifications without parsing or retaining transcript text, or starting
/// another server.
pub fn spawn_rollout_activity_monitor() -> Receiver<HashSet<String>> {
    let (sender, receiver) = mpsc::channel();
    let Some(sessions_root) = codex_sessions_root() else {
        return receiver;
    };

    let _ = thread::Builder::new()
        .name("codex-rollout-activity".into())
        .spawn(move || {
            let mut monitor = RolloutActivityMonitor::default();
            let mut projection_maintenance = ProjectionMaintenance::default();
            let mut app_server_pids = Vec::new();
            let mut tick = 0_u8;

            loop {
                if tick == 0 {
                    app_server_pids = find_app_server_pids();
                }
                tick = (tick + 1) % APP_SERVER_REFRESH_TICKS;

                let open_rollouts =
                    open_rollout_paths(&sessions_root, app_server_pids.iter().copied());
                projection_maintenance.repair_changed(&open_rollouts, &sessions_root);
                if let Some(snapshot) = monitor.scan_paths(open_rollouts) {
                    tracing::debug!(
                        running_threads = snapshot.len(),
                        "local rollout activity changed"
                    );
                    if sender.send(snapshot).is_err() {
                        break;
                    }
                }
                thread::sleep(Duration::from_secs(1));
            }
        });

    receiver
}

#[derive(Debug)]
struct ProjectionAttempt {
    rollout_size: u64,
    pending: bool,
    failed: bool,
    retry_at: Instant,
    last_error: Option<String>,
}

impl ProjectionAttempt {
    fn should_retry(&self, rollout_size: u64, now: Instant) -> bool {
        self.pending || self.rollout_size != rollout_size || (self.failed && now >= self.retry_at)
    }
}

#[derive(Debug, Default)]
struct ProjectionMaintenance {
    attempts: HashMap<PathBuf, ProjectionAttempt>,
}

impl ProjectionMaintenance {
    fn repair_changed(&mut self, open_paths: &HashSet<PathBuf>, sessions_root: &Path) {
        self.attempts.retain(|path, _| open_paths.contains(path));
        let Some(codex_home) = sessions_root.parent() else {
            return;
        };
        let now = Instant::now();
        for path in open_paths {
            let Ok(metadata) = fs::metadata(path) else {
                continue;
            };
            let rollout_size = metadata.len();
            if self
                .attempts
                .get(path)
                .is_some_and(|attempt| !attempt.should_retry(rollout_size, now))
            {
                continue;
            }
            let Some(thread_id) = thread_id_from_rollout_path(path) else {
                continue;
            };
            match crate::history::repair_projection(&thread_id, path, codex_home, rollout_size) {
                Ok(repair) => {
                    if repair.status == "repaired" {
                        tracing::info!(
                            %thread_id,
                            projected_turns = repair.projected_turns,
                            projected_items = repair.projected_items,
                            skipped_items = repair.skipped_items,
                            next_ordinal = repair.next_ordinal,
                            "maintained the shared Codex history projection in the background"
                        );
                    }
                    if repair.has_more {
                        tracing::debug!(
                            %thread_id,
                            next_ordinal = repair.next_ordinal,
                            "shared Codex history projection repair continues in the next maintenance pass"
                        );
                    }
                    self.attempts.insert(
                        path.clone(),
                        ProjectionAttempt {
                            rollout_size,
                            pending: repair.has_more,
                            failed: false,
                            retry_at: now,
                            last_error: None,
                        },
                    );
                }
                Err(error) => {
                    let message = error.to_string();
                    let changed = self
                        .attempts
                        .get(path)
                        .and_then(|attempt| attempt.last_error.as_deref())
                        != Some(message.as_str());
                    if changed {
                        tracing::warn!(
                            %thread_id,
                            path = %path.display(),
                            %error,
                            "background Codex history projection maintenance was safely skipped"
                        );
                    }
                    self.attempts.insert(
                        path.clone(),
                        ProjectionAttempt {
                            rollout_size,
                            pending: false,
                            failed: true,
                            retry_at: now + PROJECTION_RETRY_DELAY,
                            last_error: Some(message),
                        },
                    );
                }
            }
        }
    }
}

fn codex_sessions_root() -> Option<PathBuf> {
    env::var_os("CODEX_HOME")
        .map(PathBuf::from)
        .or_else(|| dirs::home_dir().map(|home| home.join(".codex")))
        .map(|root| root.join("sessions"))
}

fn find_app_server_pids() -> Vec<u32> {
    let Ok(entries) = fs::read_dir("/proc") else {
        return Vec::new();
    };
    entries
        .flatten()
        .filter_map(|entry| {
            let pid = entry.file_name().to_string_lossy().parse::<u32>().ok()?;
            let cmdline = fs::read(entry.path().join("cmdline")).ok()?;
            cmdline_is_app_server(&cmdline).then_some(pid)
        })
        .collect()
}

fn cmdline_is_app_server(cmdline: &[u8]) -> bool {
    contains_bytes(cmdline, b"codex") && contains_bytes(cmdline, b"app-server")
}

fn open_rollout_paths(
    sessions_root: &Path,
    app_server_pids: impl Iterator<Item = u32>,
) -> HashSet<PathBuf> {
    let mut paths = HashSet::new();
    for pid in app_server_pids {
        let Ok(entries) = fs::read_dir(format!("/proc/{pid}/fd")) else {
            continue;
        };
        for entry in entries.flatten() {
            let Ok(target) = fs::read_link(entry.path()) else {
                continue;
            };
            if target.starts_with(sessions_root) && thread_id_from_rollout_path(&target).is_some() {
                paths.insert(target);
            }
        }
    }
    paths
}

pub fn open_app_server_rollout_paths() -> HashSet<PathBuf> {
    let Some(sessions_root) = codex_sessions_root() else {
        return HashSet::new();
    };
    open_rollout_paths(&sessions_root, find_app_server_pids().into_iter())
}

#[derive(Debug, Default)]
struct RolloutActivityMonitor {
    files: HashMap<PathBuf, RolloutState>,
    last_snapshot: Option<HashSet<String>>,
}

impl RolloutActivityMonitor {
    fn scan_paths(&mut self, open_paths: HashSet<PathBuf>) -> Option<HashSet<String>> {
        self.files.retain(|path, _| open_paths.contains(path));
        for path in &open_paths {
            if let Err(error) = self.update_path(path) {
                self.files.remove(path);
                tracing::debug!(path = %path.display(), %error, "rollout activity probe skipped");
            }
        }

        let snapshot = self
            .files
            .values()
            .filter(|state| state.running)
            .map(|state| state.thread_id.clone())
            .collect::<HashSet<_>>();
        if self.last_snapshot.as_ref() == Some(&snapshot) {
            None
        } else {
            self.last_snapshot = Some(snapshot.clone());
            Some(snapshot)
        }
    }

    fn update_path(&mut self, path: &Path) -> io::Result<()> {
        let len = fs::metadata(path)?.len();
        let Some(state) = self.files.get_mut(path) else {
            self.files
                .insert(path.to_owned(), RolloutState::load(path, len)?);
            return Ok(());
        };

        if len < state.offset {
            *state = RolloutState::load(path, len)?;
        } else if len > state.offset {
            state.read_appended(path)?;
        }
        Ok(())
    }
}

#[derive(Debug)]
struct RolloutState {
    thread_id: String,
    offset: u64,
    running: bool,
    overlap: Vec<u8>,
}

impl RolloutState {
    fn load(path: &Path, len: u64) -> io::Result<Self> {
        let thread_id = thread_id_from_rollout_path(path).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "invalid rollout filename")
        })?;
        Ok(Self {
            thread_id,
            offset: len,
            running: latest_marker_in_file(path, len)?.unwrap_or(false),
            overlap: read_tail(path, len, MARKER_OVERLAP_BYTES)?,
        })
    }

    fn read_appended(&mut self, path: &Path) -> io::Result<()> {
        let mut file = File::open(path)?;
        file.seek(SeekFrom::Start(self.offset))?;
        let mut chunk = vec![0_u8; READ_CHUNK_BYTES];
        loop {
            let read = file.read(&mut chunk)?;
            if read == 0 {
                break;
            }
            self.offset += read as u64;
            let mut combined = Vec::with_capacity(self.overlap.len() + read);
            combined.extend_from_slice(&self.overlap);
            combined.extend_from_slice(&chunk[..read]);
            if let Some(running) = latest_marker(&combined) {
                self.running = running;
            }
            self.overlap = tail_bytes(&combined, MARKER_OVERLAP_BYTES);
        }
        Ok(())
    }
}

fn latest_marker_in_file(path: &Path, len: u64) -> io::Result<Option<bool>> {
    let mut file = File::open(path)?;
    let mut end = len;
    let mut later_prefix = Vec::new();
    while end > 0 {
        let read_len =
            usize::try_from(end.min(READ_CHUNK_BYTES as u64)).unwrap_or(READ_CHUNK_BYTES);
        let start = end - read_len as u64;
        file.seek(SeekFrom::Start(start))?;
        let mut chunk = vec![0_u8; read_len];
        file.read_exact(&mut chunk)?;

        let prefix_len = chunk.len().min(MARKER_OVERLAP_BYTES);
        let current_prefix = chunk[..prefix_len].to_vec();
        chunk.extend_from_slice(&later_prefix);
        if let Some(running) = latest_marker(&chunk) {
            return Ok(Some(running));
        }
        later_prefix = current_prefix;
        end = start;
    }
    Ok(None)
}

fn latest_marker(bytes: &[u8]) -> Option<bool> {
    let started = rfind_bytes(bytes, TASK_STARTED_MARKER);
    let completed = rfind_bytes(bytes, TASK_COMPLETE_MARKER);
    match (started, completed) {
        (Some(started), Some(completed)) => Some(started > completed),
        (Some(_), None) => Some(true),
        (None, Some(_)) => Some(false),
        (None, None) => None,
    }
}

fn read_tail(path: &Path, len: u64, count: usize) -> io::Result<Vec<u8>> {
    let mut file = File::open(path)?;
    let read_len = usize::try_from(len.min(count as u64)).unwrap_or(count);
    file.seek(SeekFrom::Start(len - read_len as u64))?;
    let mut bytes = vec![0_u8; read_len];
    file.read_exact(&mut bytes)?;
    Ok(bytes)
}

fn tail_bytes(bytes: &[u8], count: usize) -> Vec<u8> {
    bytes[bytes.len().saturating_sub(count)..].to_vec()
}

fn rfind_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    memmem::rfind(haystack, needle)
}

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    memmem::find(haystack, needle).is_some()
}

fn thread_id_from_rollout_path(path: &Path) -> Option<String> {
    let filename = path.file_name()?.to_str()?;
    let stem = filename.strip_suffix(".jsonl")?;
    let candidate = stem.get(stem.len().checked_sub(36)?..)?;
    uuid::Uuid::parse_str(candidate).ok()?;
    Some(candidate.to_owned())
}

#[cfg(test)]
mod tests {
    use std::{
        fs::{self, OpenOptions},
        io::Write,
    };

    use super::*;

    const THREAD_ID: &str = "019f7128-bec1-7543-b0c0-7d5d6b4a0542";

    #[test]
    fn rollout_filename_recovers_the_thread_id() {
        let path = Path::new(
            "/tmp/sessions/2026/07/18/rollout-2026-07-18T12-00-00-019f7128-bec1-7543-b0c0-7d5d6b4a0542.jsonl",
        );
        assert_eq!(
            thread_id_from_rollout_path(path).as_deref(),
            Some(THREAD_ID)
        );
        assert!(thread_id_from_rollout_path(Path::new("rollout-invalid.jsonl")).is_none());
    }

    #[test]
    fn projection_attempts_retry_growth_and_back_off_unchanged_failures() {
        let now = Instant::now();
        let success = ProjectionAttempt {
            rollout_size: 100,
            pending: false,
            failed: false,
            retry_at: now,
            last_error: None,
        };
        assert!(!success.should_retry(100, now + Duration::from_secs(10)));
        assert!(success.should_retry(101, now));

        let failure = ProjectionAttempt {
            rollout_size: 100,
            pending: false,
            failed: true,
            retry_at: now + PROJECTION_RETRY_DELAY,
            last_error: Some("busy".into()),
        };
        assert!(!failure.should_retry(100, now));
        assert!(failure.should_retry(101, now));
        assert!(failure.should_retry(100, now + PROJECTION_RETRY_DELAY));

        let pending = ProjectionAttempt {
            rollout_size: 100,
            pending: true,
            failed: false,
            retry_at: now,
            last_error: None,
        };
        assert!(pending.should_retry(100, now));
    }

    #[test]
    fn latest_lifecycle_marker_controls_running_state() {
        assert_eq!(
            latest_marker(br#"{"payload":{"type":"task_started"}}"#),
            Some(true)
        );
        assert_eq!(
            latest_marker(
                br#"{"payload":{"type":"task_started"}}\n{"payload":{"type":"task_complete"}}"#
            ),
            Some(false)
        );
        assert_eq!(
            latest_marker(
                br#"{"payload":{"type":"task_complete"}}\n{"payload":{"type":"task_started"}}"#
            ),
            Some(true)
        );
    }

    #[test]
    fn monitor_tracks_started_completed_and_restarted_rollouts() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp
            .path()
            .join(format!("rollout-2026-07-18T12-00-00-{THREAD_ID}.jsonl"));
        fs::write(&path, "{\"payload\":{\"type\":\"task_started\"}}\n").unwrap();

        let mut monitor = RolloutActivityMonitor::default();
        let snapshot = monitor.scan_paths(HashSet::from([path.clone()])).unwrap();
        assert!(snapshot.contains(THREAD_ID));

        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        writeln!(file, "{{\"payload\":{{\"type\":\"task_complete\"}}}}").unwrap();
        let snapshot = monitor.scan_paths(HashSet::from([path.clone()])).unwrap();
        assert!(!snapshot.contains(THREAD_ID));

        writeln!(file, "{{\"payload\":{{\"type\":\"task_started\"}}}}").unwrap();
        let snapshot = monitor.scan_paths(HashSet::from([path])).unwrap();
        assert!(snapshot.contains(THREAD_ID));

        let snapshot = monitor.scan_paths(HashSet::new()).unwrap();
        assert!(!snapshot.contains(THREAD_ID));
    }

    #[test]
    fn process_probe_requires_codex_app_server_argv() {
        assert!(cmdline_is_app_server(
            b"/usr/bin/codex\0app-server\0--stdio\0"
        ));
        assert!(!cmdline_is_app_server(b"/usr/bin/codex\0exec\0"));
        assert!(!cmdline_is_app_server(b"/usr/bin/unrelated\0app-server\0"));
    }
}
