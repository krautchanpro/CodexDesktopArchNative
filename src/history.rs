use std::{
    collections::HashSet,
    fs,
    io::{BufRead, BufReader, Read, Seek},
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, bail};
use chrono::{DateTime, Utc};
use rusqlite::{
    Connection, OpenFlags, OptionalExtension, Transaction, TransactionBehavior, backup::Backup,
    params,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// Keep each projection repair transaction bounded. An unread rollout gap may
/// be arbitrarily large; maintenance advances it one batch at a time.
pub(crate) const MAX_REPAIR_SUFFIX_BYTES: u64 = 128 * 1024 * 1024;
const MAX_REPAIRED_ITEM_JSON_BYTES: usize = 2 * 1024 * 1024;
const MAX_REPAIRED_COMMAND_OUTPUT_BYTES: usize = 512 * 1024;
const MAX_NEUTRAL_CURSOR_GAP_RECORDS: usize = 64;
const MAX_CURSOR_LOOKBACK_BYTES: u64 = 2 * 1024 * 1024;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectionRepair {
    pub status: String,
    pub activated: bool,
    pub projected_turns: usize,
    pub projected_items: usize,
    pub skipped_items: usize,
    pub next_ordinal: Option<u64>,
    pub has_more: bool,
    pub detail: Option<String>,
}

impl ProjectionRepair {
    pub fn skipped(detail: String) -> Self {
        Self {
            status: "skipped".into(),
            activated: false,
            projected_turns: 0,
            projected_items: 0,
            skipped_items: 0,
            next_ordinal: None,
            has_more: false,
            detail: Some(detail),
        }
    }

    fn current() -> Self {
        Self {
            status: "current".into(),
            activated: false,
            projected_turns: 0,
            projected_items: 0,
            skipped_items: 0,
            next_ordinal: None,
            has_more: false,
            detail: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RepairMarker {
    thread_id: String,
    database_schema: String,
    activated_at: DateTime<Utc>,
    backup: PathBuf,
}

#[derive(Debug)]
struct ProjectionState {
    byte_offset: u64,
    next_ordinal: u64,
}

#[derive(Debug)]
struct RolloutRecord {
    start_offset: u64,
    end_offset: u64,
    ordinal: u64,
    timestamp_ms: i64,
    value: Value,
}

#[derive(Debug)]
struct RolloutSuffix {
    skipped_prefix: Vec<RolloutRecord>,
    records: Vec<RolloutRecord>,
    complete_end_offset: u64,
    skipped_cursor_neutral: bool,
    has_more: bool,
}

#[derive(Debug)]
enum HistoryChange {
    Turn {
        turn_id: String,
        ordinal: u64,
        start_offset: u64,
        end_offset: u64,
        status: &'static str,
        error_json: Option<String>,
        started_at: Option<i64>,
        completed_at: Option<i64>,
        duration_ms: Option<i64>,
    },
    Item {
        turn_id: String,
        item_id: String,
        ordinal: u64,
        created_at_ms: i64,
        item_type: String,
        item_json: String,
    },
}

#[derive(Debug)]
struct ProjectionSchema {
    signature: String,
    item_updated_ordinal: bool,
    turn_offsets: bool,
}

pub fn repair_projection(
    thread_id: &str,
    rollout_path: &Path,
    codex_home: &Path,
    _rollout_size: u64,
) -> anyhow::Result<ProjectionRepair> {
    repair_projection_with_limit(
        thread_id,
        rollout_path,
        codex_home,
        _rollout_size,
        MAX_REPAIR_SUFFIX_BYTES,
    )
}

fn repair_projection_with_limit(
    thread_id: &str,
    rollout_path: &Path,
    codex_home: &Path,
    _rollout_size: u64,
    max_batch_bytes: u64,
) -> anyhow::Result<ProjectionRepair> {
    let database = codex_home.join("thread_history_1.sqlite");
    if !database.is_file() {
        return Ok(ProjectionRepair::current());
    }
    let marker_path = marker_path(codex_home, thread_id);
    let marker = read_marker(&marker_path)?;
    let connection = Connection::open_with_flags(
        &database,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("failed to open {}", database.display()))?;
    connection.busy_timeout(Duration::from_secs(3))?;
    let schema = projection_schema(&connection)?;
    if marker
        .as_ref()
        .is_some_and(|marker| marker.database_schema != schema.signature)
    {
        bail!("Codex changed the history database schema; automatic repair stayed read-only");
    }
    let Some(state) = projection_state(&connection, thread_id)? else {
        return Ok(ProjectionRepair::current());
    };
    let suffix = read_rollout_suffix_with_limit(
        rollout_path,
        state.byte_offset,
        state.next_ordinal,
        max_batch_bytes,
    )?;
    if suffix.complete_end_offset == state.byte_offset {
        return Ok(ProjectionRepair::current());
    }
    let activated = marker.is_some();
    let repair_triggered_by_cursor = suffix.skipped_cursor_neutral;
    let first = suffix
        .records
        .first()
        .context("rollout grew without a complete canonical record")?;
    let skipped_neutral_gap = first
        .ordinal
        .checked_sub(state.next_ordinal)
        .and_then(|gap| usize::try_from(gap).ok())
        .is_some_and(|gap| {
            gap > 0
                && gap <= MAX_NEUTRAL_CURSOR_GAP_RECORDS
                && suffix.skipped_prefix.len() == gap
                && suffix
                    .skipped_prefix
                    .last()
                    .is_some_and(|record| record.end_offset == state.byte_offset)
                && suffix
                    .skipped_prefix
                    .iter()
                    .enumerate()
                    .all(|(index, record)| {
                        record.ordinal
                            == state
                                .next_ordinal
                                .saturating_add(u64::try_from(index).unwrap_or(u64::MAX))
                            && is_projection_neutral(&record.value)
                    })
        });
    if !activated && !skipped_neutral_gap && !repair_triggered_by_cursor {
        return Ok(ProjectionRepair::current());
    }
    if first.ordinal != state.next_ordinal && !skipped_neutral_gap {
        bail!(
            "history cursor is not a proven bounded neutral projection gap (expected {}, found {})",
            state.next_ordinal,
            first.ordinal
        );
    }
    validate_contiguous_ordinals(&suffix.records)?;

    let mut changes = Vec::new();
    let mut skipped_items = 0;
    for record in &suffix.records {
        match project_record(thread_id, record)? {
            Some(change) => changes.push(change),
            None if is_item_completed(&record.value) => skipped_items += 1,
            None => {}
        }
    }
    let next_ordinal = suffix
        .records
        .last()
        .and_then(|record| record.ordinal.checked_add(1))
        .context("rollout ordinal overflow")?;

    let marker = match marker {
        Some(marker) => marker,
        None => {
            let backup = backup_database(&connection, codex_home)?;
            let marker = RepairMarker {
                thread_id: thread_id.to_owned(),
                database_schema: schema.signature.clone(),
                activated_at: Utc::now(),
                backup,
            };
            write_marker(&marker_path, &marker)?;
            marker
        }
    };
    drop(marker);

    let mut connection = connection;
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let current = projection_state_transaction(&transaction, thread_id)?
        .context("history projection state disappeared before repair")?;
    if current.byte_offset != state.byte_offset || current.next_ordinal != state.next_ordinal {
        transaction.rollback()?;
        return Ok(ProjectionRepair::current());
    }
    let mut projected_turns = 0;
    let mut projected_items = 0;
    for change in changes {
        match change {
            HistoryChange::Turn { .. } => {
                apply_turn(&transaction, thread_id, &schema, change)?;
                projected_turns += 1;
            }
            HistoryChange::Item { .. } => {
                apply_item(&transaction, thread_id, &schema, change)?;
                projected_items += 1;
            }
        }
    }
    refresh_turn_summary_ids(&transaction, thread_id)?;
    transaction.execute(
        "UPDATE thread_history_projection_state
         SET next_rollout_byte_offset = ?, next_rollout_ordinal = ?
         WHERE thread_id = ?",
        params![
            i64::try_from(suffix.complete_end_offset)?,
            i64::try_from(next_ordinal)?,
            thread_id
        ],
    )?;
    transaction.commit()?;
    Ok(ProjectionRepair {
        status: "repaired".into(),
        activated: !activated,
        projected_turns,
        projected_items,
        skipped_items,
        next_ordinal: Some(next_ordinal),
        has_more: suffix.has_more,
        detail: None,
    })
}

fn marker_path(codex_home: &Path, thread_id: &str) -> PathBuf {
    codex_home
        .join("codex-native/history-projection-repair")
        .join(format!("{thread_id}.json"))
}

fn read_marker(path: &Path) -> anyhow::Result<Option<RepairMarker>> {
    if !path.is_file() {
        return Ok(None);
    }
    let bytes = fs::read(path)
        .with_context(|| format!("failed to read projection repair marker {}", path.display()))?;
    serde_json::from_slice(&bytes)
        .map(Some)
        .with_context(|| format!("projection repair marker is invalid: {}", path.display()))
}

fn write_marker(path: &Path, marker: &RepairMarker) -> anyhow::Result<()> {
    let parent = path.parent().context("projection marker has no parent")?;
    fs::create_dir_all(parent)?;
    let temporary = path.with_extension("json.new");
    fs::write(&temporary, serde_json::to_vec_pretty(marker)?)?;
    fs::rename(&temporary, path)?;
    Ok(())
}

fn backup_database(connection: &Connection, codex_home: &Path) -> anyhow::Result<PathBuf> {
    let root = codex_home.join("backups/codex-native-history");
    fs::create_dir_all(&root)?;
    let path = root.join(format!(
        "thread_history_1-{}.sqlite",
        Utc::now().format("%Y%m%dT%H%M%S%.3fZ")
    ));
    let mut destination = Connection::open(&path)?;
    let backup = Backup::new(connection, &mut destination)?;
    backup.run_to_completion(128, Duration::from_millis(10), None)?;
    drop(backup);
    destination.close().map_err(|(_, error)| error)?;
    Ok(path)
}

fn projection_schema(connection: &Connection) -> anyhow::Result<ProjectionSchema> {
    let state = table_columns(connection, "thread_history_projection_state")?;
    let items = table_columns(connection, "thread_items")?;
    let turns = table_columns(connection, "thread_turns")?;
    for required in [
        "thread_id",
        "next_rollout_byte_offset",
        "next_rollout_ordinal",
    ] {
        if !state.contains(required) {
            bail!("unsupported Codex projection-state schema");
        }
    }
    for required in [
        "thread_id",
        "turn_id",
        "item_id",
        "rollout_ordinal",
        "created_at_ms",
        "item_type",
        "item_json",
    ] {
        if !items.contains(required) {
            bail!("unsupported Codex thread-item schema");
        }
    }
    for required in [
        "thread_id",
        "turn_id",
        "rollout_ordinal",
        "status",
        "error_json",
        "started_at",
        "completed_at",
        "duration_ms",
        "first_user_item_id",
        "final_agent_item_id",
    ] {
        if !turns.contains(required) {
            bail!("unsupported Codex thread-turn schema");
        }
    }
    let item_updated_ordinal = items.contains("updated_at_ordinal");
    let turn_offsets = turns.contains("rollout_byte_offset")
        && turns.contains("rollout_end_ordinal")
        && turns.contains("rollout_end_byte_offset");
    let signature = format!(
        "state:{};items:{};turns:{}",
        sorted_columns(&state),
        sorted_columns(&items),
        sorted_columns(&turns)
    );
    Ok(ProjectionSchema {
        signature,
        item_updated_ordinal,
        turn_offsets,
    })
}

fn table_columns(connection: &Connection, table: &str) -> anyhow::Result<HashSet<String>> {
    let mut statement = connection.prepare(&format!("PRAGMA table_info({table})"))?;
    let rows = statement.query_map([], |row| row.get::<_, String>(1))?;
    let columns = rows.collect::<Result<HashSet<_>, _>>()?;
    if columns.is_empty() {
        bail!("Codex history table {table} is missing");
    }
    Ok(columns)
}

fn sorted_columns(columns: &HashSet<String>) -> String {
    let mut values = columns.iter().map(String::as_str).collect::<Vec<_>>();
    values.sort_unstable();
    values.join(",")
}

fn projection_state(
    connection: &Connection,
    thread_id: &str,
) -> anyhow::Result<Option<ProjectionState>> {
    connection
        .query_row(
            "SELECT next_rollout_byte_offset, next_rollout_ordinal
             FROM thread_history_projection_state WHERE thread_id = ?",
            [thread_id],
            |row| {
                Ok(ProjectionState {
                    byte_offset: u64::try_from(row.get::<_, i64>(0)?).map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            0,
                            rusqlite::types::Type::Integer,
                            Box::new(error),
                        )
                    })?,
                    next_ordinal: u64::try_from(row.get::<_, i64>(1)?).map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            1,
                            rusqlite::types::Type::Integer,
                            Box::new(error),
                        )
                    })?,
                })
            },
        )
        .optional()
        .map_err(Into::into)
}

fn projection_state_transaction(
    transaction: &Transaction<'_>,
    thread_id: &str,
) -> anyhow::Result<Option<ProjectionState>> {
    transaction
        .query_row(
            "SELECT next_rollout_byte_offset, next_rollout_ordinal
             FROM thread_history_projection_state WHERE thread_id = ?",
            [thread_id],
            |row| {
                Ok(ProjectionState {
                    byte_offset: row.get::<_, i64>(0)?.try_into().map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            0,
                            rusqlite::types::Type::Integer,
                            Box::new(error),
                        )
                    })?,
                    next_ordinal: row.get::<_, i64>(1)?.try_into().map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            1,
                            rusqlite::types::Type::Integer,
                            Box::new(error),
                        )
                    })?,
                })
            },
        )
        .optional()
        .map_err(Into::into)
}

#[cfg(test)]
fn read_rollout_suffix(
    path: &Path,
    start_offset: u64,
    next_ordinal: u64,
) -> anyhow::Result<RolloutSuffix> {
    read_rollout_suffix_with_limit(path, start_offset, next_ordinal, MAX_REPAIR_SUFFIX_BYTES)
}

fn read_rollout_suffix_with_limit(
    path: &Path,
    start_offset: u64,
    next_ordinal: u64,
    max_batch_bytes: u64,
) -> anyhow::Result<RolloutSuffix> {
    if max_batch_bytes == 0 {
        bail!("projection-repair batch limit must be positive");
    }
    let mut file = fs::File::open(path)?;
    let file_len = file.metadata()?.len();
    if start_offset > file_len {
        bail!("Codex history cursor is beyond the rollout file");
    }
    let scan_start = rollout_scan_start(&mut file, start_offset)?;
    file.seek(std::io::SeekFrom::Start(scan_start))?;
    let mut reader = BufReader::new(file);
    let mut offset = scan_start;
    let mut skipped_prefix = Vec::new();
    let mut records = Vec::new();
    let batch_end = start_offset
        .checked_add(max_batch_bytes)
        .context("projection-repair batch offset overflow")?;
    let mut complete_end_offset = start_offset;
    let mut skipped_cursor_neutral = false;
    let mut bytes = Vec::new();
    let mut hit_batch_limit = false;
    loop {
        if offset >= batch_end {
            hit_batch_limit = true;
            break;
        }
        let remaining = if offset < start_offset {
            max_batch_bytes
        } else {
            batch_end.saturating_sub(offset)
        };
        let line = read_bounded_rollout_line(&mut reader, &mut bytes, remaining)?;
        let Some(line) = line else {
            break;
        };
        let count = match line {
            BoundedRolloutLine::Complete(count) => count,
            BoundedRolloutLine::Partial { at_limit } => {
                if offset < start_offset {
                    bail!(
                        "rollout record overlaps the cursor and exceeds the guarded record limit"
                    );
                }
                if offset == start_offset
                    && at_limit
                    && file_len > offset.saturating_add(max_batch_bytes)
                {
                    bail!("rollout record exceeds the guarded record limit");
                }
                hit_batch_limit = at_limit;
                break;
            }
        };
        let end = offset
            .checked_add(u64::try_from(count)?)
            .context("rollout offset overflow")?;
        complete_end_offset = end;
        if bytes.iter().all(u8::is_ascii_whitespace) {
            offset = end;
            continue;
        }
        let value: Value = serde_json::from_slice(&bytes)
            .with_context(|| format!("invalid complete rollout line at byte {offset}"))?;
        let ordinal = value
            .get("ordinal")
            .and_then(Value::as_u64)
            .with_context(|| format!("rollout line at byte {offset} has no ordinal"))?;
        let timestamp_ms = value
            .get("timestamp")
            .and_then(Value::as_str)
            .and_then(|timestamp| DateTime::parse_from_rfc3339(timestamp).ok())
            .map_or(0, |timestamp| timestamp.timestamp_millis());
        let record = RolloutRecord {
            start_offset: offset,
            end_offset: end,
            ordinal,
            timestamp_ms,
            value,
        };
        let overlaps_cursor = offset < start_offset && start_offset < end;
        let redundant_neutral = records.is_empty()
            && ordinal.checked_add(1) == Some(next_ordinal)
            && is_projection_neutral(&record.value);
        if overlaps_cursor {
            if !redundant_neutral {
                bail!("Codex history cursor points inside a non-neutral rollout record");
            }
            skipped_cursor_neutral = true;
        } else if end <= start_offset {
            if ordinal >= next_ordinal {
                if skipped_prefix.len() >= MAX_NEUTRAL_CURSOR_GAP_RECORDS {
                    bail!("history cursor neutral-gap candidate exceeds the guarded limit");
                }
                skipped_prefix.push(record);
            }
        } else if offset >= start_offset {
            if redundant_neutral {
                skipped_cursor_neutral = true;
                offset = end;
                continue;
            }
            records.push(record);
        }
        offset = end;
    }
    if start_offset > complete_end_offset {
        bail!("Codex history cursor is beyond the complete rollout");
    }
    Ok(RolloutSuffix {
        skipped_prefix,
        records,
        complete_end_offset,
        skipped_cursor_neutral,
        has_more: hit_batch_limit && file_len > complete_end_offset,
    })
}

enum BoundedRolloutLine {
    Complete(usize),
    Partial { at_limit: bool },
}

fn read_bounded_rollout_line(
    reader: &mut BufReader<fs::File>,
    bytes: &mut Vec<u8>,
    max_bytes: u64,
) -> anyhow::Result<Option<BoundedRolloutLine>> {
    let max_bytes = usize::try_from(max_bytes).context("projection-repair batch is too large")?;
    bytes.clear();
    loop {
        let buffer = reader.fill_buf()?;
        if buffer.is_empty() {
            return Ok(
                (!bytes.is_empty()).then_some(BoundedRolloutLine::Partial { at_limit: false })
            );
        }
        if bytes.len() >= max_bytes {
            return Ok(Some(BoundedRolloutLine::Partial { at_limit: true }));
        }
        let newline = buffer.iter().position(|byte| *byte == b'\n');
        let available = newline.map_or(buffer.len(), |index| index + 1);
        if bytes.len().saturating_add(available) > max_bytes {
            return Ok(Some(BoundedRolloutLine::Partial { at_limit: true }));
        }
        bytes.extend_from_slice(&buffer[..available]);
        reader.consume(available);
        if newline.is_some() {
            return Ok(Some(BoundedRolloutLine::Complete(bytes.len())));
        }
    }
}

fn rollout_scan_start(file: &mut fs::File, start_offset: u64) -> anyhow::Result<u64> {
    if start_offset == 0 {
        return Ok(0);
    }
    let window_start = start_offset.saturating_sub(MAX_CURSOR_LOOKBACK_BYTES);
    file.seek(std::io::SeekFrom::Start(window_start))?;
    let window_len = usize::try_from(start_offset - window_start)
        .context("history cursor lookback is too large")?;
    let mut window = vec![0_u8; window_len];
    file.read_exact(&mut window)?;
    let newline_positions = window
        .iter()
        .enumerate()
        .filter_map(|(index, byte)| (*byte == b'\n').then_some(index));
    let positions = newline_positions.collect::<Vec<_>>();
    if let Some(index) = positions
        .len()
        .checked_sub(MAX_NEUTRAL_CURSOR_GAP_RECORDS + 1)
        .and_then(|index| positions.get(index))
    {
        return Ok(window_start + u64::try_from(index + 1)?);
    }
    if window_start == 0 {
        Ok(0)
    } else if let Some(index) = positions.first() {
        Ok(window_start + u64::try_from(index + 1)?)
    } else {
        bail!("Codex history cursor has no bounded preceding line")
    }
}

fn validate_contiguous_ordinals(records: &[RolloutRecord]) -> anyhow::Result<()> {
    let mut neutral_duplicates = 0_usize;
    for pair in records.windows(2) {
        if pair[1].ordinal == pair[0].ordinal
            && (is_projection_neutral(&pair[0].value) || is_projection_neutral(&pair[1].value))
        {
            neutral_duplicates += 1;
            if neutral_duplicates > MAX_NEUTRAL_CURSOR_GAP_RECORDS {
                bail!("canonical rollout has too many neutral duplicate ordinals");
            }
            continue;
        }
        if pair[1].ordinal != pair[0].ordinal.saturating_add(1) {
            bail!(
                "canonical rollout has a non-contiguous ordinal gap ({} to {})",
                pair[0].ordinal,
                pair[1].ordinal
            );
        }
    }
    Ok(())
}

fn is_projection_neutral(value: &Value) -> bool {
    matches!(
        value.pointer("/payload/type").and_then(Value::as_str),
        Some("token_count" | "thread_settings_applied")
    )
}

fn is_item_completed(value: &Value) -> bool {
    value.pointer("/payload/type").and_then(Value::as_str) == Some("item_completed")
}

fn project_record(
    thread_id: &str,
    record: &RolloutRecord,
) -> anyhow::Result<Option<HistoryChange>> {
    let payload = record.value.get("payload").unwrap_or(&Value::Null);
    let kind = payload.get("type").and_then(Value::as_str);
    let turn_id = payload
        .get("turn_id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    match kind {
        Some("task_started") if !turn_id.is_empty() => Ok(Some(HistoryChange::Turn {
            turn_id,
            ordinal: record.ordinal,
            start_offset: record.start_offset,
            end_offset: record.end_offset,
            status: "inProgress",
            error_json: None,
            started_at: payload.get("started_at").and_then(Value::as_i64),
            completed_at: None,
            duration_ms: None,
        })),
        Some("task_complete") if !turn_id.is_empty() => {
            let error = payload.get("error").filter(|value| !value.is_null());
            Ok(Some(HistoryChange::Turn {
                turn_id,
                ordinal: record.ordinal,
                start_offset: record.start_offset,
                end_offset: record.end_offset,
                status: if error.is_some() {
                    "failed"
                } else {
                    "completed"
                },
                error_json: error.map(serde_json::to_string).transpose()?,
                started_at: payload.get("started_at").and_then(Value::as_i64),
                completed_at: payload.get("completed_at").and_then(Value::as_i64),
                duration_ms: payload.get("duration_ms").and_then(Value::as_i64),
            }))
        }
        Some("turn_aborted") if !turn_id.is_empty() => Ok(Some(HistoryChange::Turn {
            turn_id,
            ordinal: record.ordinal,
            start_offset: record.start_offset,
            end_offset: record.end_offset,
            status: "interrupted",
            error_json: None,
            started_at: payload.get("started_at").and_then(Value::as_i64),
            completed_at: payload.get("completed_at").and_then(Value::as_i64),
            duration_ms: payload.get("duration_ms").and_then(Value::as_i64),
        })),
        Some("item_completed")
            if payload.get("thread_id").and_then(Value::as_str) == Some(thread_id)
                && !turn_id.is_empty() =>
        {
            let Some(item) = payload.get("item").and_then(project_thread_item) else {
                return Ok(None);
            };
            let item_id = item
                .get("id")
                .and_then(Value::as_str)
                .context("projected history item has no ID")?
                .to_owned();
            let item_type = item
                .get("type")
                .and_then(Value::as_str)
                .context("projected history item has no type")?
                .to_owned();
            let item_json = serde_json::to_string(&item)?;
            if item_json.len() > MAX_REPAIRED_ITEM_JSON_BYTES {
                return Ok(None);
            }
            Ok(Some(HistoryChange::Item {
                turn_id,
                item_id,
                ordinal: record.ordinal,
                created_at_ms: payload
                    .get("completed_at_ms")
                    .and_then(Value::as_i64)
                    .unwrap_or(record.timestamp_ms),
                item_type,
                item_json,
            }))
        }
        _ => Ok(None),
    }
}

fn project_thread_item(item: &Value) -> Option<Value> {
    let item_type = item.get("type").and_then(Value::as_str)?;
    let id = item.get("id").and_then(Value::as_str)?;
    match item_type {
        "UserMessage" => Some(json!({
            "type": "userMessage",
            "id": id,
            "clientId": item.get("client_id").cloned().unwrap_or(Value::Null),
            "content": item.get("content").cloned().unwrap_or_else(|| json!([]))
        })),
        "AgentMessage" => Some(json!({
            "type": "agentMessage",
            "id": id,
            "text": joined_agent_text(item),
            "phase": item.get("phase").cloned().unwrap_or(Value::Null),
            "memoryCitation": item.get("memory_citation").cloned().unwrap_or(Value::Null)
        })),
        "Reasoning" => Some(json!({
            "type": "reasoning",
            "id": id,
            "summary": item.get("summary_text").cloned().unwrap_or_else(|| json!([])),
            "content": item.get("raw_content").cloned().unwrap_or_else(|| json!([]))
        })),
        "Plan" => Some(json!({
            "type": "plan",
            "id": id,
            "text": item.get("text").cloned().unwrap_or_else(|| json!(""))
        })),
        "CommandExecution" => project_command_item(item, id),
        "McpToolCall" => Some(project_mcp_item(item, id)),
        "FileChange" => project_file_change_item(item, id),
        "Extension" if item.get("kind").and_then(Value::as_str) == Some("web.search") => {
            let mut projected = item.as_object()?.clone();
            projected.remove("kind");
            projected.insert("type".into(), json!("webSearch"));
            Some(Value::Object(projected))
        }
        "ImageView" => Some(json!({
            "type": "imageView",
            "id": id,
            "path": normalize_file_path(item.get("path").and_then(Value::as_str).unwrap_or_default())
        })),
        "ContextCompaction" => Some(json!({"type":"contextCompaction","id":id})),
        _ => None,
    }
}

fn joined_agent_text(item: &Value) -> String {
    item.get("content")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|part| part.get("text").and_then(Value::as_str))
        .collect::<String>()
}

fn project_command_item(item: &Value, id: &str) -> Option<Value> {
    let command = item
        .get("command")
        .and_then(Value::as_array)
        .map(|arguments| {
            shell_join(
                &arguments
                    .iter()
                    .filter_map(Value::as_str)
                    .collect::<Vec<_>>(),
            )
        })
        .or_else(|| {
            item.get("command")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })?;
    let mut output = item
        .get("aggregated_output")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .unwrap_or_else(|| {
            [
                item.get("stdout")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
                item.get("stderr")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
            ]
            .concat()
        });
    if output.len() > MAX_REPAIRED_COMMAND_OUTPUT_BYTES {
        output.truncate(output.floor_char_boundary(MAX_REPAIRED_COMMAND_OUTPUT_BYTES));
    }
    Some(json!({
        "type": "commandExecution",
        "id": id,
        "pluginId": item.get("plugin_id").cloned().unwrap_or(Value::Null),
        "scriptPath": item.get("script_path").cloned().unwrap_or(Value::Null),
        "command": command,
        "cwd": normalize_file_path(item.get("cwd").and_then(Value::as_str).unwrap_or_default()),
        "processId": item.get("process_id").cloned().unwrap_or(Value::Null),
        "source": lower_camel(item.get("source").and_then(Value::as_str).unwrap_or("agent")),
        "status": lower_camel(item.get("status").and_then(Value::as_str).unwrap_or("completed")),
        "commandActions": [],
        "aggregatedOutput": if output.is_empty() { Value::Null } else { Value::String(output) },
        "exitCode": item.get("exit_code").cloned().unwrap_or(Value::Null),
        "durationMs": duration_ms(item.get("duration"))
    }))
}

fn project_mcp_item(item: &Value, id: &str) -> Value {
    let connector = item.get("connector_id").and_then(Value::as_str);
    let result = item.get("result").filter(|value| !value.is_null()).map(|result| {
        json!({
            "content": result.get("content").cloned().unwrap_or_else(|| json!([])),
            "structuredContent": result.get("structured_content").cloned().unwrap_or(Value::Null),
            "_meta": result.get("_meta").cloned().unwrap_or(Value::Null)
        })
    });
    json!({
        "type": "mcpToolCall",
        "id": id,
        "server": item.get("server").cloned().unwrap_or_else(|| json!("")),
        "tool": item.get("tool").cloned().unwrap_or_else(|| json!("")),
        "status": lower_camel(item.get("status").and_then(Value::as_str).unwrap_or("completed")),
        "arguments": item.get("arguments").cloned().unwrap_or_else(|| json!({})),
        "appContext": connector.map(|connector_id| json!({
            "connectorId": connector_id,
            "linkId": item.get("link_id").cloned().unwrap_or(Value::Null),
            "resourceUri": item.get("mcp_app_resource_uri").cloned().unwrap_or(Value::Null),
            "appName": item.get("app_name").cloned().unwrap_or(Value::Null),
            "actionName": item.get("action_name").cloned().unwrap_or(Value::Null)
        })).unwrap_or(Value::Null),
        "pluginId": item.get("plugin_id").cloned().unwrap_or(Value::Null),
        "result": result.unwrap_or(Value::Null),
        "error": item.get("error").cloned().unwrap_or(Value::Null),
        "durationMs": duration_ms(item.get("duration"))
    })
}

fn project_file_change_item(item: &Value, id: &str) -> Option<Value> {
    let mut changes = Vec::new();
    for (path, change) in item.get("changes")?.as_object()? {
        let kind = change.get("type").and_then(Value::as_str)?;
        let mut kind_value = json!({"type": kind});
        if let Some(move_path) = change.get("move_path") {
            kind_value["move_path"] = move_path.clone();
        }
        changes.push(json!({
            "path": path,
            "kind": kind_value,
            "diff": change.get("content")
                .or_else(|| change.get("unified_diff"))
                .cloned()
                .unwrap_or_else(|| json!(""))
        }));
    }
    Some(json!({
        "type": "fileChange",
        "id": id,
        "changes": changes,
        "status": lower_camel(item.get("status").and_then(Value::as_str).unwrap_or("in_progress"))
    }))
}

fn duration_ms(value: Option<&Value>) -> Value {
    let Some(value) = value else {
        return Value::Null;
    };
    if let Some(milliseconds) = value.as_i64() {
        return json!(milliseconds);
    }
    let seconds = value.get("secs").and_then(Value::as_i64).unwrap_or(0);
    let nanos = value.get("nanos").and_then(Value::as_i64).unwrap_or(0);
    json!(
        seconds
            .saturating_mul(1000)
            .saturating_add(nanos / 1_000_000)
    )
}

fn normalize_file_path(value: &str) -> String {
    value.strip_prefix("file://").unwrap_or(value).to_owned()
}

fn lower_camel(value: &str) -> String {
    let mut parts = value.split('_');
    let mut output = parts.next().unwrap_or_default().to_owned();
    for part in parts {
        let mut characters = part.chars();
        if let Some(first) = characters.next() {
            output.extend(first.to_uppercase());
            output.extend(characters);
        }
    }
    output
}

fn shell_join(arguments: &[&str]) -> String {
    arguments
        .iter()
        .map(|argument| {
            if !argument.is_empty()
                && argument.chars().all(|character| {
                    character.is_ascii_alphanumeric() || "_-./:=+".contains(character)
                })
            {
                (*argument).to_owned()
            } else {
                format!("'{}'", argument.replace('\'', "'\"'\"'"))
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn apply_turn(
    transaction: &Transaction<'_>,
    thread_id: &str,
    schema: &ProjectionSchema,
    change: HistoryChange,
) -> anyhow::Result<()> {
    let HistoryChange::Turn {
        turn_id,
        ordinal,
        start_offset,
        end_offset,
        status,
        error_json,
        started_at,
        completed_at,
        duration_ms,
    } = change
    else {
        unreachable!();
    };
    if schema.turn_offsets {
        let terminal = status != "inProgress";
        transaction.execute(
            "INSERT INTO thread_turns (
                thread_id, turn_id, rollout_ordinal, rollout_byte_offset,
                rollout_end_ordinal, rollout_end_byte_offset, status, error_json,
                started_at, completed_at, duration_ms
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(thread_id, turn_id) DO UPDATE SET
                rollout_end_ordinal=excluded.rollout_end_ordinal,
                rollout_end_byte_offset=excluded.rollout_end_byte_offset,
                status=excluded.status, error_json=excluded.error_json,
                started_at=excluded.started_at, completed_at=excluded.completed_at,
                duration_ms=excluded.duration_ms",
            params![
                thread_id,
                turn_id,
                i64::try_from(ordinal)?,
                i64::try_from(start_offset)?,
                terminal.then_some(i64::try_from(ordinal)?),
                terminal.then_some(i64::try_from(end_offset)?),
                status,
                error_json,
                started_at,
                completed_at,
                duration_ms
            ],
        )?;
    } else {
        transaction.execute(
            "INSERT INTO thread_turns (
                thread_id, turn_id, rollout_ordinal, status, error_json,
                started_at, completed_at, duration_ms
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(thread_id, turn_id) DO UPDATE SET
                status=excluded.status, error_json=excluded.error_json,
                started_at=excluded.started_at, completed_at=excluded.completed_at,
                duration_ms=excluded.duration_ms",
            params![
                thread_id,
                turn_id,
                i64::try_from(ordinal)?,
                status,
                error_json,
                started_at,
                completed_at,
                duration_ms
            ],
        )?;
    }
    Ok(())
}

fn apply_item(
    transaction: &Transaction<'_>,
    thread_id: &str,
    schema: &ProjectionSchema,
    change: HistoryChange,
) -> anyhow::Result<()> {
    let HistoryChange::Item {
        turn_id,
        item_id,
        ordinal,
        created_at_ms,
        item_type,
        item_json,
    } = change
    else {
        unreachable!();
    };
    if schema.item_updated_ordinal {
        transaction.execute(
            "INSERT INTO thread_items (
                thread_id, turn_id, item_id, rollout_ordinal, updated_at_ordinal,
                created_at_ms, item_type, item_json
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(thread_id, turn_id, item_id) DO UPDATE SET
                updated_at_ordinal=excluded.updated_at_ordinal,
                item_type=excluded.item_type, item_json=excluded.item_json",
            params![
                thread_id,
                turn_id,
                item_id,
                i64::try_from(ordinal)?,
                i64::try_from(ordinal)?,
                created_at_ms,
                item_type,
                item_json
            ],
        )?;
    } else {
        transaction.execute(
            "INSERT INTO thread_items (
                thread_id, turn_id, item_id, rollout_ordinal, created_at_ms,
                item_type, item_json
             ) VALUES (?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(thread_id, turn_id, item_id) DO UPDATE SET
                item_type=excluded.item_type, item_json=excluded.item_json",
            params![
                thread_id,
                turn_id,
                item_id,
                i64::try_from(ordinal)?,
                created_at_ms,
                item_type,
                item_json
            ],
        )?;
    }
    Ok(())
}

fn refresh_turn_summary_ids(transaction: &Transaction<'_>, thread_id: &str) -> anyhow::Result<()> {
    transaction.execute(
        "UPDATE thread_turns
         SET first_user_item_id = COALESCE(
             first_user_item_id,
             (SELECT item_id FROM thread_items
              WHERE thread_id = thread_turns.thread_id
                AND turn_id = thread_turns.turn_id
                AND item_type = 'userMessage'
              ORDER BY rollout_ordinal LIMIT 1)
         ),
         final_agent_item_id = COALESCE(
             (SELECT item_id FROM thread_items
              WHERE thread_id = thread_turns.thread_id
                AND turn_id = thread_turns.turn_id
                AND item_type = 'agentMessage'
                AND json_extract(item_json, '$.phase') = 'final_answer'
              ORDER BY rollout_ordinal DESC LIMIT 1),
             CASE WHEN status IN ('completed','interrupted','failed') THEN
                 (SELECT item_id FROM thread_items
                  WHERE thread_id = thread_turns.thread_id
                    AND turn_id = thread_turns.turn_id
                    AND item_type = 'agentMessage'
                    AND json_extract(item_json, '$.phase') IS NULL
                  ORDER BY rollout_ordinal DESC LIMIT 1)
             END,
             final_agent_item_id
         )
         WHERE thread_id = ?",
        [thread_id],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Seek, SeekFrom, Write};
    use tempfile::tempdir;

    #[test]
    fn repairs_multiple_transactional_batches_without_rollout_growth() {
        let directory = tempdir().expect("tempdir");
        let codex_home = directory.path().join(".codex");
        fs::create_dir_all(&codex_home).expect("codex home");
        let thread_id = "00000000-0000-0000-0000-000000000001";
        let rollout = codex_home
            .join("sessions/2026/07/26/rollout-test-00000000-0000-0000-0000-000000000001.jsonl");
        fs::create_dir_all(rollout.parent().expect("rollout parent")).expect("sessions");
        let records = (0..3)
            .map(|ordinal| {
                serde_json::to_string(&json!({
                    "timestamp":"2026-07-26T00:00:00Z","ordinal":ordinal,"type":"event_msg",
                    "payload":{"type":"task_started","turn_id":format!("turn-{ordinal}"),"started_at":1}
                }))
                .expect("record") + "\n"
            })
            .collect::<Vec<_>>();
        fs::write(&rollout, records.concat()).expect("rollout");
        let database = create_projection_test_database(&codex_home, thread_id, 0, 0);
        let schema = {
            let connection = Connection::open(&database).expect("database");
            projection_schema(&connection).expect("schema").signature
        };
        write_marker(
            &marker_path(&codex_home, thread_id),
            &RepairMarker {
                thread_id: thread_id.to_owned(),
                database_schema: schema,
                activated_at: Utc::now(),
                backup: directory.path().join("backup.sqlite"),
            },
        )
        .expect("marker");
        let batch_limit = u64::try_from(records[0].len()).expect("batch limit");
        let rollout_size = fs::metadata(&rollout).expect("metadata").len();

        let first = repair_projection_with_limit(
            thread_id,
            &rollout,
            &codex_home,
            rollout_size,
            batch_limit,
        )
        .expect("first repair");
        assert_eq!(first.next_ordinal, Some(1));
        assert!(first.has_more);
        assert_eq!(first.projected_turns, 1);
        assert_eq!(projection_test_state(&database), (batch_limit, 1));

        let second = repair_projection_with_limit(
            thread_id,
            &rollout,
            &codex_home,
            rollout_size,
            batch_limit,
        )
        .expect("second repair");
        assert_eq!(second.next_ordinal, Some(2));
        assert!(second.has_more);
        assert_eq!(second.projected_turns, 1);
        assert_eq!(projection_test_state(&database), (batch_limit * 2, 2));

        let third = repair_projection_with_limit(
            thread_id,
            &rollout,
            &codex_home,
            rollout_size,
            batch_limit,
        )
        .expect("third repair");
        assert_eq!(third.next_ordinal, Some(3));
        assert!(!third.has_more);
        assert_eq!(third.projected_turns, 1);
        assert_eq!(projection_test_state(&database), (rollout_size, 3));
        let connection = Connection::open(&database).expect("database");
        assert_eq!(
            connection
                .query_row("SELECT COUNT(*) FROM thread_turns", [], |row| {
                    row.get::<_, i64>(0)
                })
                .expect("turn count"),
            3
        );
    }

    #[test]
    fn oversized_record_leaves_projection_cursor_unchanged() {
        let directory = tempdir().expect("tempdir");
        let codex_home = directory.path().join(".codex");
        fs::create_dir_all(&codex_home).expect("codex home");
        let thread_id = "00000000-0000-0000-0000-000000000001";
        let rollout = codex_home
            .join("sessions/2026/07/26/rollout-test-00000000-0000-0000-0000-000000000001.jsonl");
        fs::create_dir_all(rollout.parent().expect("rollout parent")).expect("sessions");
        let record = serde_json::to_string(&json!({
            "timestamp":"2026-07-26T00:00:00Z","ordinal":0,"type":"event_msg",
            "payload":{"type":"token_count","info":"x".repeat(128),"rate_limits":null}
        }))
        .expect("record")
            + "\n";
        fs::write(&rollout, record).expect("rollout");
        let database = create_projection_test_database(&codex_home, thread_id, 0, 0);
        let error = repair_projection_with_limit(
            thread_id,
            &rollout,
            &codex_home,
            fs::metadata(&rollout).expect("metadata").len(),
            32,
        )
        .expect_err("oversized record");
        assert!(
            error
                .to_string()
                .contains("exceeds the guarded record limit")
        );
        assert_eq!(projection_test_state(&database), (0, 0));
    }

    fn create_projection_test_database(
        codex_home: &Path,
        thread_id: &str,
        byte_offset: u64,
        next_ordinal: u64,
    ) -> PathBuf {
        let database = codex_home.join("thread_history_1.sqlite");
        let connection = Connection::open(&database).expect("database");
        connection
            .execute_batch(
                "CREATE TABLE thread_history_projection_state (
                    thread_id TEXT PRIMARY KEY,
                    next_rollout_byte_offset INTEGER NOT NULL,
                    next_rollout_ordinal INTEGER NOT NULL
                 );
                 CREATE TABLE thread_items (
                    thread_id TEXT NOT NULL, turn_id TEXT NOT NULL, item_id TEXT NOT NULL,
                    rollout_ordinal INTEGER NOT NULL, created_at_ms INTEGER NOT NULL,
                    item_json TEXT NOT NULL, item_type TEXT NOT NULL DEFAULT '',
                    PRIMARY KEY(thread_id,turn_id,item_id)
                 );
                 CREATE TABLE thread_turns (
                    thread_id TEXT NOT NULL, turn_id TEXT NOT NULL,
                    rollout_ordinal INTEGER NOT NULL, status TEXT NOT NULL,
                    error_json TEXT, started_at INTEGER, completed_at INTEGER,
                    duration_ms INTEGER, first_user_item_id TEXT, final_agent_item_id TEXT,
                    PRIMARY KEY(thread_id,turn_id)
                 );",
            )
            .expect("schema");
        connection
            .execute(
                "INSERT INTO thread_history_projection_state VALUES(?,?,?)",
                params![
                    thread_id,
                    i64::try_from(byte_offset).expect("offset"),
                    i64::try_from(next_ordinal).expect("ordinal")
                ],
            )
            .expect("state");
        database
    }

    fn projection_test_state(database: &Path) -> (u64, u64) {
        let connection = Connection::open(database).expect("database");
        connection
            .query_row(
                "SELECT next_rollout_byte_offset, next_rollout_ordinal
                 FROM thread_history_projection_state",
                [],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?.try_into().expect("offset"),
                        row.get::<_, i64>(1)?.try_into().expect("ordinal"),
                    ))
                },
            )
            .expect("projection state")
    }

    #[test]
    fn projects_chat_and_tool_items_to_app_server_shape() {
        assert_eq!(
            project_thread_item(&json!({
                "type":"AgentMessage",
                "id":"agent",
                "content":[{"type":"Text","text":"hello "},{"type":"Text","text":"world"}],
                "phase":"final_answer"
            })),
            Some(json!({
                "type":"agentMessage",
                "id":"agent",
                "text":"hello world",
                "phase":"final_answer",
                "memoryCitation":null
            }))
        );
        let command = project_thread_item(&json!({
            "type":"CommandExecution",
            "id":"exec",
            "command":["bash","-lc","printf 'ok'"],
            "cwd":"file:///tmp/work",
            "source":"unified_exec_startup",
            "status":"completed",
            "stdout":"ok",
            "duration":{"secs":1,"nanos":500000000}
        }))
        .expect("command projects");
        assert_eq!(command["command"], "bash -lc 'printf '\"'\"'ok'\"'\"''");
        assert_eq!(command["cwd"], "/tmp/work");
        assert_eq!(command["source"], "unifiedExecStartup");
        assert_eq!(command["durationMs"], 1500);
    }

    #[test]
    fn repairs_only_a_proven_bounded_run_of_neutral_projection_gaps() {
        let directory = tempdir().expect("tempdir");
        let codex_home = directory.path().join(".codex");
        fs::create_dir_all(&codex_home).expect("codex home");
        let rollout = codex_home
            .join("sessions/2026/07/26/rollout-test-00000000-0000-0000-0000-000000000001.jsonl");
        fs::create_dir_all(rollout.parent().expect("rollout parent")).expect("sessions");
        let first = serde_json::to_string(&json!({
            "timestamp":"2026-07-26T00:00:00Z","ordinal":0,"type":"event_msg",
            "payload":{"type":"token_count","info":null,"rate_limits":null}
        }))
        .expect("first")
            + "\n";
        let second = serde_json::to_string(&json!({
            "timestamp":"2026-07-26T00:00:01Z","ordinal":1,"type":"event_msg",
            "payload":{"type":"token_count","info":null,"rate_limits":null}
        }))
        .expect("second")
            + "\n";
        let third = serde_json::to_string(&json!({
            "timestamp":"2026-07-26T00:00:02Z","ordinal":2,"type":"event_msg",
            "payload":{"type":"task_started","turn_id":"turn-1","started_at":1}
        }))
        .expect("third")
            + "\n";
        let fourth = serde_json::to_string(&json!({
            "timestamp":"2026-07-26T00:00:03Z","ordinal":3,"type":"event_msg",
            "payload":{
                "type":"item_completed",
                "thread_id":"00000000-0000-0000-0000-000000000001",
                "turn_id":"turn-1",
                "item":{"type":"UserMessage","id":"user-1","content":[{"type":"text","text":"hello"}]},
                "completed_at_ms":2
            }
        }))
        .expect("fourth")
            + "\n";
        fs::write(&rollout, format!("{first}{second}{third}{fourth}")).expect("rollout");
        let database = codex_home.join("thread_history_1.sqlite");
        let connection = Connection::open(&database).expect("database");
        connection
            .execute_batch(
                "CREATE TABLE thread_history_projection_state (
                    thread_id TEXT PRIMARY KEY,
                    next_rollout_byte_offset INTEGER NOT NULL,
                    next_rollout_ordinal INTEGER NOT NULL
                 );
                 CREATE TABLE thread_items (
                    thread_id TEXT NOT NULL, turn_id TEXT NOT NULL, item_id TEXT NOT NULL,
                    rollout_ordinal INTEGER NOT NULL, created_at_ms INTEGER NOT NULL,
                    item_json TEXT NOT NULL, item_type TEXT NOT NULL DEFAULT '',
                    PRIMARY KEY(thread_id,turn_id,item_id)
                 );
                 CREATE TABLE thread_turns (
                    thread_id TEXT NOT NULL, turn_id TEXT NOT NULL,
                    rollout_ordinal INTEGER NOT NULL, status TEXT NOT NULL,
                    error_json TEXT, started_at INTEGER, completed_at INTEGER,
                    duration_ms INTEGER, first_user_item_id TEXT, final_agent_item_id TEXT,
                    PRIMARY KEY(thread_id,turn_id)
                 );",
            )
            .expect("schema");
        connection
            .execute(
                "INSERT INTO thread_history_projection_state VALUES(?,?,?)",
                params![
                    "00000000-0000-0000-0000-000000000001",
                    i64::try_from(first.len() + second.len()).expect("offset"),
                    0_i64
                ],
            )
            .expect("state");
        drop(connection);

        let result = repair_projection(
            "00000000-0000-0000-0000-000000000001",
            &rollout,
            &codex_home,
            fs::metadata(&rollout).expect("metadata").len(),
        )
        .expect("repair");
        assert_eq!(result.status, "repaired");
        assert!(result.activated);
        assert_eq!(result.projected_turns, 1);
        assert_eq!(result.projected_items, 1);
        let connection = Connection::open(&database).expect("database");
        assert_eq!(
            connection
                .query_row(
                    "SELECT next_rollout_ordinal FROM thread_history_projection_state",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .expect("ordinal"),
            4
        );
        assert_eq!(
            connection
                .query_row("SELECT item_type FROM thread_items", [], |row| {
                    row.get::<_, String>(0)
                })
                .expect("item"),
            "userMessage"
        );
    }

    #[test]
    fn allows_a_large_rollout_when_only_a_small_tail_is_unread() {
        let directory = tempdir().expect("tempdir");
        let rollout = directory.path().join("large-rollout.jsonl");
        let start_offset = MAX_REPAIR_SUFFIX_BYTES + 1024;
        let mut file = fs::File::create(&rollout).expect("rollout");
        file.set_len(start_offset).expect("sparse prefix");
        file.seek(SeekFrom::Start(start_offset - 1))
            .expect("cursor");
        file.write_all(b"\n").expect("prefix newline");
        let record = serde_json::to_string(&json!({
            "timestamp":"2026-07-26T00:00:00Z","ordinal":0,"type":"event_msg",
            "payload":{"type":"token_count","info":null,"rate_limits":null}
        }))
        .expect("record")
            + "\n";
        file.write_all(record.as_bytes()).expect("tail");
        file.flush().expect("flush");

        let suffix = read_rollout_suffix(&rollout, start_offset, 0).expect("bounded suffix");
        assert_eq!(suffix.records.len(), 1);
        assert_eq!(suffix.records[0].ordinal, 0);
        assert!(fs::metadata(&rollout).expect("metadata").len() > MAX_REPAIR_SUFFIX_BYTES);
    }

    #[test]
    fn rejects_a_record_larger_than_the_batch_limit() {
        let directory = tempdir().expect("tempdir");
        let rollout = directory.path().join("oversized-suffix.jsonl");
        fs::File::create(&rollout)
            .expect("rollout")
            .set_len(MAX_REPAIR_SUFFIX_BYTES + 1)
            .expect("sparse suffix");

        let error = read_rollout_suffix(&rollout, 0, 0).expect_err("oversized suffix");
        assert!(
            error
                .to_string()
                .contains("exceeds the guarded record limit")
        );
    }

    #[test]
    fn advances_an_unlimited_gap_in_bounded_complete_record_batches() {
        let directory = tempdir().expect("tempdir");
        let rollout = directory.path().join("chunked-rollout.jsonl");
        let records = (0..3)
            .map(|ordinal| {
                serde_json::to_string(&json!({
                    "timestamp":"2026-07-26T00:00:00Z","ordinal":ordinal,"type":"event_msg",
                    "payload":{"type":"token_count","info":null,"rate_limits":null}
                }))
                .expect("record")
                    + "\n"
            })
            .collect::<Vec<_>>();
        fs::write(&rollout, records.concat()).expect("rollout");
        let batch_limit = u64::try_from(records[0].len()).expect("batch limit");

        let first =
            read_rollout_suffix_with_limit(&rollout, 0, 0, batch_limit).expect("first batch");
        assert_eq!(first.records.len(), 1);
        assert_eq!(first.complete_end_offset, batch_limit);
        assert!(first.has_more);

        let second =
            read_rollout_suffix_with_limit(&rollout, first.complete_end_offset, 1, batch_limit)
                .expect("second batch");
        assert_eq!(second.records.len(), 1);
        assert_eq!(second.records[0].ordinal, 1);
        assert!(second.has_more);

        let third =
            read_rollout_suffix_with_limit(&rollout, second.complete_end_offset, 2, batch_limit)
                .expect("third batch");
        assert_eq!(third.records.len(), 1);
        assert_eq!(third.records[0].ordinal, 2);
        assert!(!third.has_more);
    }

    #[test]
    fn accepts_a_cursor_on_a_redundant_neutral_record() {
        let directory = tempdir().expect("tempdir");
        let rollout = directory.path().join("overlapping-neutral.jsonl");
        let records = [
            json!({
                "timestamp":"2026-07-26T00:00:00Z","ordinal":0,"type":"event_msg",
                "payload":{"type":"task_started","turn_id":"turn-0"}
            }),
            json!({
                "timestamp":"2026-07-26T00:00:01Z","ordinal":1,"type":"event_msg",
                "payload":{"type":"thread_settings_applied"}
            }),
            json!({
                "timestamp":"2026-07-26T00:00:02Z","ordinal":2,"type":"event_msg",
                "payload":{"type":"task_started","turn_id":"turn-1"}
            }),
        ];
        let mut bytes = Vec::new();
        let mut offsets = Vec::new();
        for record in records {
            offsets.push(u64::try_from(bytes.len()).expect("offset"));
            bytes.extend_from_slice(serde_json::to_string(&record).expect("record").as_bytes());
            bytes.push(b'\n');
        }
        fs::write(&rollout, bytes).expect("rollout");

        let suffix = read_rollout_suffix(&rollout, offsets[1], 2).expect("overlap");
        assert_eq!(suffix.records.len(), 1);
        assert_eq!(suffix.records[0].ordinal, 2);
        assert_eq!(suffix.records[0].start_offset, offsets[2]);
    }

    #[test]
    #[ignore = "requires an explicitly isolated CODEX_NATIVE_HISTORY_TEST_HOME copy"]
    fn external_projection_copy_can_be_repaired_for_manual_diagnostics() {
        let codex_home = std::env::var_os("CODEX_NATIVE_HISTORY_TEST_HOME")
            .map(PathBuf::from)
            .expect("CODEX_NATIVE_HISTORY_TEST_HOME must name an isolated copy");
        let rollout = std::env::var_os("CODEX_NATIVE_HISTORY_TEST_ROLLOUT")
            .map(PathBuf::from)
            .expect("CODEX_NATIVE_HISTORY_TEST_ROLLOUT is required");
        let thread_id = std::env::var("CODEX_NATIVE_HISTORY_TEST_THREAD")
            .expect("CODEX_NATIVE_HISTORY_TEST_THREAD is required");
        let result = repair_projection(
            &thread_id,
            &rollout,
            &codex_home,
            fs::metadata(&rollout).expect("rollout metadata").len(),
        )
        .expect("guarded projection repair");
        println!(
            "{}",
            serde_json::to_string_pretty(&result).expect("serialize repair result")
        );
    }
}
