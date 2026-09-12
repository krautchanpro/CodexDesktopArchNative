use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

use anyhow::{Context, anyhow};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::persistence::{config_dir, read_json, write_json_atomic};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Automation {
    pub id: Uuid,
    pub title: String,
    pub prompt: String,
    pub cwd: PathBuf,
    /// A systemd OnCalendar expression such as `Mon..Fri *-*-* 09:00:00`.
    pub schedule: String,
    #[serde(default)]
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AutomationRun {
    pub id: Uuid,
    pub automation_id: Uuid,
    pub trigger: String,
    pub status: String,
    pub started_at: DateTime<Utc>,
    #[serde(default)]
    pub completed_at: Option<DateTime<Utc>>,
    pub cwd: PathBuf,
    pub log_path: PathBuf,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct AutomationHistory {
    #[serde(default)]
    pub runs: Vec<AutomationRun>,
}

impl AutomationHistory {
    pub fn path() -> anyhow::Result<PathBuf> {
        Ok(config_dir()?.join("automation-history.json"))
    }

    pub fn load() -> Self {
        Self::path()
            .and_then(read_json)
            .unwrap_or_else(|_| Self::default())
    }

    pub fn save(&self) -> anyhow::Result<()> {
        write_json_atomic(&Self::path()?, self)
    }

    fn upsert(&mut self, run: AutomationRun) {
        const MAX_RUNS: usize = 200;
        if let Some(existing) = self.runs.iter_mut().find(|item| item.id == run.id) {
            *existing = run;
        } else {
            self.runs.push(run);
        }
        self.runs
            .sort_by_key(|item| std::cmp::Reverse(item.started_at));
        self.runs.truncate(MAX_RUNS);
    }

    pub fn for_automation(&self, id: Uuid) -> Vec<&AutomationRun> {
        self.runs
            .iter()
            .filter(|run| run.automation_id == id)
            .collect()
    }
}

impl Automation {
    pub fn new(
        title: impl Into<String>,
        prompt: impl Into<String>,
        cwd: PathBuf,
        schedule: impl Into<String>,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            title: title.into(),
            prompt: prompt.into(),
            cwd,
            schedule: schedule.into(),
            enabled: false,
            created_at: Utc::now(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AutomationStore {
    #[serde(default)]
    pub items: Vec<Automation>,
}

impl AutomationStore {
    pub fn path() -> anyhow::Result<PathBuf> {
        Ok(config_dir()?.join("automations.json"))
    }

    pub fn load() -> Self {
        Self::path()
            .and_then(read_json)
            .unwrap_or_else(|_| Self::default())
    }

    pub fn save(&self) -> anyhow::Result<()> {
        write_json_atomic(&Self::path()?, self)
    }

    pub fn upsert(&mut self, automation: Automation) {
        if let Some(existing) = self.items.iter_mut().find(|item| item.id == automation.id) {
            *existing = automation;
        } else {
            self.items.push(automation);
        }
    }

    pub fn remove(&mut self, id: Uuid) {
        self.items.retain(|item| item.id != id);
    }
}

pub fn systemd_unit_name(id: Uuid) -> String {
    format!("codex-native-automation-{id}")
}

pub fn render_service(automation: &Automation, executable: &Path) -> anyhow::Result<String> {
    let executable = systemd_quote(executable.to_string_lossy().as_ref())?;
    let cwd = systemd_quote(automation.cwd.to_string_lossy().as_ref())?;
    Ok(format!(
        "[Unit]\nDescription=Codex Native automation: {}\n\n[Service]\nType=oneshot\nExecStart={} --run-automation {}\nWorkingDirectory={}\nNice=10\n",
        sanitize_description(&automation.title),
        executable,
        automation.id,
        cwd,
    ))
}

pub fn render_timer(automation: &Automation) -> String {
    format!(
        "[Unit]\nDescription=Schedule {}\n\n[Timer]\nOnCalendar={}\nPersistent=true\nRandomizedDelaySec=15\n\n[Install]\nWantedBy=timers.target\n",
        sanitize_description(&automation.title),
        automation.schedule.trim(),
    )
}

pub fn install_systemd_units(automation: &Automation, executable: &Path) -> anyhow::Result<()> {
    validate_schedule(&automation.schedule)?;
    let user_dir = dirs::config_dir()
        .context("XDG config directory unavailable")?
        .join("systemd/user");
    fs::create_dir_all(&user_dir)?;
    let name = systemd_unit_name(automation.id);
    fs::write(
        user_dir.join(format!("{name}.service")),
        render_service(automation, executable)?,
    )?;
    fs::write(
        user_dir.join(format!("{name}.timer")),
        render_timer(automation),
    )?;

    run_systemctl(&["daemon-reload"])?;
    if automation.enabled {
        run_systemctl(&["enable", "--now", &format!("{name}.timer")])?;
    }
    Ok(())
}

pub fn remove_systemd_units(id: Uuid) -> anyhow::Result<()> {
    let user_dir = dirs::config_dir()
        .context("XDG config directory unavailable")?
        .join("systemd/user");
    let name = systemd_unit_name(id);
    let _ = run_systemctl(&["disable", "--now", &format!("{name}.timer")]);
    for suffix in ["service", "timer"] {
        let path = user_dir.join(format!("{name}.{suffix}"));
        if path.exists() {
            fs::remove_file(&path)
                .with_context(|| format!("failed to remove {}", path.display()))?;
        }
    }
    run_systemctl(&["daemon-reload"])
}

pub fn run_automation(id: Uuid) -> anyhow::Result<()> {
    run_automation_with_trigger(id, "schedule", true)
}

pub fn run_automation_now(id: Uuid) -> anyhow::Result<()> {
    run_automation_with_trigger(id, "manual", false)
}

pub fn start_automation_now(id: Uuid) -> anyhow::Result<()> {
    let executable = std::env::current_exe().context("current executable is unavailable")?;
    Command::new(executable)
        .args(["--run-automation-now", &id.to_string()])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .context("failed to start automation")?;
    Ok(())
}

fn run_automation_with_trigger(
    id: Uuid,
    trigger: &str,
    require_enabled: bool,
) -> anyhow::Result<()> {
    let store = AutomationStore::load();
    let automation = store
        .items
        .iter()
        .find(|item| item.id == id)
        .with_context(|| format!("automation {id} does not exist"))?;
    if require_enabled && !automation.enabled {
        return Err(anyhow!("automation {id} is disabled"));
    }
    if !automation.cwd.is_dir() {
        return Err(anyhow!(
            "automation working directory does not exist: {}",
            automation.cwd.display()
        ));
    }

    let run_cwd = automation.cwd.clone();
    let run_id = Uuid::new_v4();
    let log_dir = dirs::state_dir()
        .or_else(dirs::data_local_dir)
        .context("XDG state directory unavailable")?
        .join("codex-native/automation-logs");
    fs::create_dir_all(&log_dir)?;
    let log_path = log_dir.join(format!("{run_id}.log"));
    let mut run = AutomationRun {
        id: run_id,
        automation_id: id,
        trigger: trigger.into(),
        status: "running".into(),
        started_at: Utc::now(),
        completed_at: None,
        cwd: run_cwd.clone(),
        log_path: log_path.clone(),
        error: None,
    };
    update_history(run.clone())?;

    let execution = (|| -> anyhow::Result<()> {
        let log = fs::File::create(&log_path)
            .with_context(|| format!("failed to create {}", log_path.display()))?;
        let log_stderr = log.try_clone()?;
        let codex = std::env::var_os("CODEX_CLI_PATH").unwrap_or_else(|| "codex".into());
        let mut child = Command::new(codex)
            .args(["exec", "--json", "-C"])
            .arg(&run_cwd)
            .arg("-")
            .stdin(Stdio::piped())
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(log_stderr))
            .spawn()
            .context("failed to start Codex automation")?;

        if let Some(mut stdin) = child.stdin.take() {
            stdin
                .write_all(automation.prompt.as_bytes())
                .context("failed to send automation prompt to Codex")?;
        }

        let status = child
            .wait()
            .context("failed to wait for Codex automation")?;
        if status.success() {
            Ok(())
        } else {
            Err(anyhow!("Codex automation exited with {status}"))
        }
    })();

    run.completed_at = Some(Utc::now());
    match execution {
        Ok(()) => {
            run.status = "completed".into();
            update_history(run)?;
            Ok(())
        }
        Err(error) => {
            let message = format!("{error:#}");
            run.status = "failed".into();
            run.error = Some(message.clone());
            if let Err(history_error) = update_history(run) {
                return Err(anyhow!(
                    "{message}; additionally failed to persist run history: {history_error:#}"
                ));
            }
            Err(error)
        }
    }
}

fn update_history(run: AutomationRun) -> anyhow::Result<()> {
    let mut history = AutomationHistory::load();
    history.upsert(run);
    history.save()
}

fn run_systemctl(args: &[&str]) -> anyhow::Result<()> {
    let output = Command::new("systemctl")
        .arg("--user")
        .args(args)
        .output()
        .context("failed to run systemctl --user")?;
    if output.status.success() {
        Ok(())
    } else {
        Err(anyhow!(
            String::from_utf8_lossy(&output.stderr).trim().to_owned()
        ))
    }
}

fn validate_schedule(schedule: &str) -> anyhow::Result<()> {
    if schedule.trim().is_empty() || schedule.contains('\n') || schedule.contains('\r') {
        return Err(anyhow!("invalid systemd calendar expression"));
    }
    Ok(())
}

fn sanitize_description(value: &str) -> String {
    value.replace(['\n', '\r'], " ")
}

fn systemd_quote(value: &str) -> anyhow::Result<String> {
    if value.contains(['\n', '\r', '\0']) {
        return Err(anyhow!("path contains unsupported control characters"));
    }
    Ok(format!(
        "\"{}\"",
        value.replace('\\', "\\\\").replace('"', "\\\"")
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn units_do_not_interpolate_prompt_or_shell() {
        let automation = Automation::new(
            "Review $(touch /tmp/nope)",
            "prompt with % and $()",
            PathBuf::from("/tmp/project with spaces"),
            "daily",
        );
        let service = render_service(&automation, Path::new("/usr/bin/codex-native")).unwrap();
        assert!(!service.contains("prompt with"));
        assert!(service.contains("--run-automation"));
        assert!(service.contains("WorkingDirectory=\"/tmp/project with spaces\""));
    }

    #[test]
    fn schedule_rejects_unit_injection() {
        assert!(validate_schedule("daily\n[Service]").is_err());
    }
}
