//! Current transcript-schema mutation and verification support.
//! The source is only inspected; mutations are applied to benchmark copies.

use super::sqlite::{Connection, Fingerprint, Result, Value, quote};
use super::virtual_table::Module;

const MARKER: &str = "\x1fzsqlite-gc-v1-5a5117e5-";

#[derive(Clone, Copy)]
enum MutationKind {
    Integer,
    Text,
    ClearText,
    SetInteger,
}

#[derive(Clone, Copy)]
struct Target {
    name: &'static str,
    table: &'static str,
    column: &'static str,
    structure: &'static str,
    index: Option<&'static str>,
    predicate: &'static str,
    order: &'static str,
    kind: MutationKind,
}

const TARGETS: &[Target] = &[
    Target {
        name: "session-visible-time",
        table: "session",
        column: "updated_at_ms",
        structure: "session_visible_updated",
        index: Some("session_visible_updated"),
        predicate: "deleted=0 AND updated_at_ms IS NOT NULL AND updated_at_ms<9223372036854775807",
        order: "updated_at_ms DESC,id DESC",
        kind: MutationKind::Integer,
    },
    Target {
        name: "event-time",
        table: "session_event",
        column: "at_ms",
        structure: "session_event_time",
        index: Some("session_event_time"),
        predicate: "at_ms IS NOT NULL AND at_ms<9223372036854775807",
        order: "session,at_ms,id",
        kind: MutationKind::Integer,
    },
    Target {
        name: "event-external-id",
        table: "session_event",
        column: "external_id",
        structure: "session_event_external",
        index: Some("session_event_external"),
        predicate: "external_id IS NOT NULL",
        order: "source,external_id,rowid",
        kind: MutationKind::ClearText,
    },
    Target {
        name: "agent-work-updated",
        table: "agent_work",
        column: "updated_at",
        structure: "agent_work_graph_state_updated/agent_work_parent_state_updated/agent_work_child_updated",
        index: Some("agent_work_graph_state_updated"),
        predicate: "updated_at<9223372036854775807",
        order: "graph,state,updated_at DESC,rowid",
        kind: MutationKind::Integer,
    },
    Target {
        name: "entry-origin",
        table: "session_entry",
        column: "origin",
        structure: "session_entry_origin",
        index: Some("session_entry_origin"),
        predicate: "origin IS NOT NULL",
        order: "origin,rowid",
        kind: MutationKind::ClearText,
    },
    Target {
        name: "tool-call-time",
        table: "tool_call",
        column: "created_at_ms",
        structure: "tool_call_session_time",
        index: Some("tool_call_session_time"),
        predicate: "created_at_ms<9223372036854775807",
        order: "session,created_at_ms,id",
        kind: MutationKind::Integer,
    },
    Target {
        name: "tool-attempt-time",
        table: "tool_attempt",
        column: "started_at_ms",
        structure: "tool_attempt_time",
        index: Some("tool_attempt_time"),
        predicate: "started_at_ms IS NULL",
        order: "started_at_ms,id",
        kind: MutationKind::SetInteger,
    },
    Target {
        name: "message-text-fts",
        table: "message_part",
        column: "text",
        structure: "message_fts/message_fts_update",
        index: None,
        predicate: "text IS NOT NULL",
        order: "rowid",
        kind: MutationKind::Text,
    },
    Target {
        name: "segment-as-of",
        table: "conversation_segment",
        column: "as_of_ms",
        structure: "conversation_segment_status",
        index: Some("conversation_segment_status"),
        predicate: "as_of_ms<9223372036854775807",
        order: "status,as_of_ms DESC,id",
        kind: MutationKind::Integer,
    },
    Target {
        name: "entity-canonical",
        table: "conversation_segment_entity",
        column: "canonical",
        structure: "conversation_segment_entity_canonical",
        index: Some("conversation_segment_entity_canonical"),
        predicate: "canonical<>''",
        order: "canonical,segment,id",
        kind: MutationKind::Text,
    },
    Target {
        name: "entity-fts-trigram",
        table: "conversation_segment_entity",
        column: "entity",
        structure: "conversation_segment_entity_fts/conversation_segment_entity_trigram",
        index: None,
        predicate: "entity<>''",
        order: "rowid",
        kind: MutationKind::Text,
    },
];

fn identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn indexed_table(target: Target) -> String {
    target.index.map_or_else(
        || identifier(target.table),
        |index| {
            format!(
                "{} INDEXED BY {}",
                identifier(target.table),
                identifier(index)
            )
        },
    )
}

fn required_objects() -> &'static [(&'static str, &'static str)] {
    &[
        ("table", "message"),
        ("table", "message_part"),
        ("table", "session"),
        ("table", "session_event"),
        ("table", "conversation_segment"),
        ("table", "conversation_segment_entity"),
        ("table", "message_fts"),
        ("table", "conversation_segment_entity_fts"),
        ("table", "conversation_segment_entity_trigram"),
        ("table", "agent_work"),
        ("table", "session_entry"),
        ("table", "tool_call"),
        ("table", "tool_attempt"),
        ("index", "agent_work_graph_state_updated"),
        ("index", "agent_work_parent_state_updated"),
        ("index", "agent_work_child_updated"),
        ("index", "session_entry_origin"),
        ("index", "tool_call_session_time"),
        ("index", "tool_attempt_time"),
        ("index", "session_visible_updated"),
        ("index", "session_event_time"),
        ("index", "session_event_external"),
        ("index", "conversation_segment_status"),
        ("index", "conversation_segment_entity_canonical"),
        ("trigger", "message_fts_update"),
        ("trigger", "conversation_segment_entity_fts_update"),
    ]
}

/// The current transcript schema is intentionally explicit. A future schema
/// must add a new plan rather than silently mutating unsuitable columns.
pub fn is_current(db: &Connection) -> Result<bool> {
    if db.scalar("PRAGMA user_version")? != 1 {
        return Ok(false);
    }
    for (kind, name) in required_objects() {
        if db.scalar(&format!(
            "SELECT count(*) FROM sqlite_schema WHERE type={} AND name={}",
            quote(kind),
            quote(name)
        ))? != 1
        {
            return Ok(false);
        }
    }
    Ok(true)
}

pub fn looks_current(db: &Connection) -> Result<bool> {
    Ok(db.scalar(
        "SELECT count(*) FROM sqlite_schema
         WHERE type='table' AND name IN ('message','message_part','session','session_event')",
    )? == 4)
}

#[derive(Clone, Debug)]
pub struct MutationCoverage {
    pub name: String,
    pub table: String,
    pub column: String,
    pub structure: String,
    pub rows: usize,
}

struct Mutation {
    forward_sql: String,
    forward: Vec<Value>,
    restore_sql: String,
    restore: Vec<Value>,
    verify_sql: String,
    verify: Vec<Value>,
    original: Fingerprint,
}

pub struct MutationPlan {
    mutations: Vec<Mutation>,
    pub coverage: Vec<MutationCoverage>,
}

fn select_sql(target: Target, suffix: &str) -> String {
    format!(
        "SELECT rowid,{} FROM {} WHERE {} {suffix}",
        identifier(target.column),
        indexed_table(target),
        target.predicate,
    )
}

/// Validate current-schema objects and one usable row per target. This performs
/// bounded index seeks only: no copy, mutation, count, integrity check, or scan.
pub fn preflight(db: &Connection) -> Result<Vec<MutationCoverage>> {
    if !is_current(db)? {
        return Err("source is not the supported current transcript schema".into());
    }
    let mut coverage = Vec::new();
    for target in TARGETS {
        let rows = db.values(&select_sql(
            *target,
            &format!("ORDER BY {} LIMIT 1", target.order),
        ))?;
        if rows.is_empty() {
            return Err(format!(
                "current transcript mutation target is empty: {}/{}",
                target.table, target.column
            )
            .into());
        }
        if rows[0].len() != 2
            || !matches!(&rows[0][0], Value::Integer(_))
            || !matches!(
                (&target.kind, &rows[0][1]),
                (MutationKind::Integer, Value::Integer(_))
                    | (MutationKind::Text | MutationKind::ClearText, Value::Text(_))
                    | (MutationKind::SetInteger, Value::Null)
            )
        {
            return Err(format!(
                "unexpected mutation sample type: {}/{}",
                target.table, target.column
            )
            .into());
        }
        coverage.push(MutationCoverage {
            name: target.name.into(),
            table: target.table.into(),
            column: target.column.into(),
            structure: target.structure.into(),
            rows: 1,
        });
    }
    Ok(coverage)
}

impl MutationPlan {
    #[allow(clippy::too_many_lines)] // Keep typed sample capture beside its reversible update.
    pub fn build(db: &Connection, samples_per_target: usize) -> Result<Self> {
        if !is_current(db)? {
            return Err("source is not the supported current transcript schema".into());
        }
        let mut mutations = Vec::new();
        let mut coverage = Vec::new();
        for target in TARGETS {
            let count = db.scalar(&format!(
                "SELECT count(*) FROM {} WHERE {}",
                indexed_table(*target),
                target.predicate
            ))?;
            if count == 0 {
                return Err(format!(
                    "current transcript mutation target is empty: {}/{}",
                    target.table, target.column
                )
                .into());
            }
            let samples = usize::try_from(count)?.min(samples_per_target.max(1));
            let mut offsets = (0..samples)
                .map(|position| {
                    if samples == 1 {
                        0
                    } else {
                        i64::try_from(position).expect("bounded sample position") * (count - 1)
                            / i64::try_from(samples - 1).expect("bounded sample count")
                    }
                })
                .collect::<Vec<_>>();
            offsets.dedup();
            let selected = offsets.len();
            for offset in offsets {
                let mut rows = db.values(&select_sql(
                    *target,
                    &format!("ORDER BY {} LIMIT 1 OFFSET {offset}", target.order),
                ))?;
                let mut row = rows.pop().ok_or("mutation sample row disappeared")?;
                if row.len() != 2 {
                    return Err("invalid mutation sample".into());
                }
                let original = row.pop().ok_or("missing mutation value")?;
                let rowid = row.pop().ok_or("missing mutation rowid")?;
                if !matches!(rowid, Value::Integer(_))
                    || !matches!(
                        (&target.kind, &original),
                        (MutationKind::Integer, Value::Integer(_))
                            | (MutationKind::Text | MutationKind::ClearText, Value::Text(_))
                            | (MutationKind::SetInteger, Value::Null)
                    )
                {
                    return Err(format!(
                        "unexpected mutation value type: {}/{}",
                        target.table, target.column
                    )
                    .into());
                }
                let Value::Integer(row_number) = rowid else {
                    return Err("mutation rowid is not an integer".into());
                };
                let rowid = Value::Integer(row_number);
                let table = identifier(target.table);
                let column = identifier(target.column);
                let verify_sql = format!("SELECT {column} FROM {table} WHERE rowid=?");
                let verify = vec![rowid.clone()];
                let original_fingerprint = db.fingerprint_params(&verify_sql, &verify)?;
                let marker = format!("{MARKER}{row_number:x}").into_bytes();
                if target.name == "entity-canonical" {
                    let Value::Text(bytes) = &original else {
                        return Err("entity canonical is not text".into());
                    };
                    let mut candidate = bytes.clone();
                    candidate.extend(&marker);
                    let collision = db.scalar_params(
                        "SELECT count(*) FROM conversation_segment_entity
                         WHERE segment=(SELECT segment FROM conversation_segment_entity WHERE rowid=?)
                           AND canonical=? AND rowid<>?",
                        &[
                            rowid.clone(),
                            Value::Text(candidate),
                            rowid.clone(),
                        ],
                    )?;
                    if collision != 0 {
                        return Err("entity canonical mutation would collide".into());
                    }
                }
                let (forward_sql, forward) = match target.kind {
                    MutationKind::Integer => (
                        format!("UPDATE {table} SET {column}={column}+? WHERE rowid=?"),
                        vec![Value::Integer(1), rowid.clone()],
                    ),
                    MutationKind::Text => (
                        format!("UPDATE {table} SET {column}={column}||? WHERE rowid=?"),
                        vec![Value::Text(marker), rowid.clone()],
                    ),
                    MutationKind::ClearText => (
                        format!("UPDATE {table} SET {column}=NULL WHERE rowid=?"),
                        vec![rowid.clone()],
                    ),
                    MutationKind::SetInteger => (
                        format!("UPDATE {table} SET {column}=? WHERE rowid=?"),
                        vec![Value::Integer(0x5a51_17e5), rowid.clone()],
                    ),
                };
                mutations.push(Mutation {
                    forward_sql,
                    forward,
                    restore_sql: format!("UPDATE {table} SET {column}=? WHERE rowid=?"),
                    restore: vec![original, rowid],
                    verify_sql,
                    verify,
                    original: original_fingerprint,
                });
            }
            coverage.push(MutationCoverage {
                name: target.name.into(),
                table: target.table.into(),
                column: target.column.into(),
                structure: target.structure.into(),
                rows: selected,
            });
        }
        Ok(Self {
            mutations,
            coverage,
        })
    }

    pub fn rows(&self) -> usize {
        self.mutations.len()
    }

    /// Even epochs apply a deterministic edit; odd epochs restore the exact
    /// typed values captured from the immutable source.
    pub fn apply(&self, db: &Connection, epoch: usize) -> Result<()> {
        let forward = epoch.is_multiple_of(2);
        db.execute("BEGIN IMMEDIATE")?;
        if forward {
            for mutation in &self.mutations {
                if db.execute_params_changes(&mutation.forward_sql, &mutation.forward)? != 1 {
                    return Err("mutation did not update exactly one row".into());
                }
            }
        } else {
            for mutation in self.mutations.iter().rev() {
                if db.execute_params_changes(&mutation.restore_sql, &mutation.restore)? != 1 {
                    return Err("restoration did not update exactly one row".into());
                }
            }
        }
        db.execute("COMMIT")?;
        if !forward {
            self.verify_restored(db)?;
        }
        Ok(())
    }

    pub fn verify_restored(&self, db: &Connection) -> Result<()> {
        for mutation in &self.mutations {
            if db.fingerprint_params(&mutation.verify_sql, &mutation.verify)? != mutation.original {
                return Err("restored mutation target differs from source".into());
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct Verification {
    pub table: String,
    pub sql: String,
}

/// Full logical verification queries for ordinary application tables and FTS5
/// instance vocabularies. These are intentionally expensive and run only at
/// the end of a full benchmark.
pub fn verification_queries(db: &Connection) -> Result<Vec<Verification>> {
    let tables = db.rows(
        "SELECT p.name,s.sql FROM pragma_table_list p
         JOIN sqlite_schema s ON s.name=p.name AND s.type='table'
         WHERE p.schema='main' AND p.name NOT LIKE 'sqlite_%' AND p.type='table'
         ORDER BY p.name",
    )?;
    let mut output = vec![
        Verification {
            table: "sqlite-schema".into(),
            sql: "SELECT type,name,tbl_name,sql FROM sqlite_schema ORDER BY type,name".into(),
        },
        Verification {
            table: "sqlite-header-pragmas".into(),
            sql: "SELECT application_id,user_version,encoding,page_size,auto_vacuum FROM pragma_application_id,pragma_user_version,pragma_encoding,pragma_page_size,pragma_auto_vacuum".into(),
        },
    ];
    for row in tables {
        let table = &row[0];
        let without_rowid = row[1].to_ascii_uppercase().contains("WITHOUT ROWID");
        let order = if without_rowid {
            let mut primary = db
                .rows(&format!(
                    "SELECT name,pk FROM pragma_table_xinfo({}) WHERE hidden=0 AND pk>0 ORDER BY pk",
                    quote(table)
                ))?
                .into_iter()
                .map(|row| Ok((row[1].parse::<i64>()?, row[0].clone())))
                .collect::<Result<Vec<_>>>()?;
            primary.sort_by_key(|(position, _)| *position);
            if primary.is_empty() {
                return Err(format!("WITHOUT ROWID table has no primary key: {table}").into());
            }
            primary
                .into_iter()
                .map(|(_, name)| identifier(&name))
                .collect::<Vec<_>>()
                .join(",")
        } else {
            "rowid".into()
        };
        output.push(Verification {
            table: table.clone(),
            sql: format!("SELECT * FROM {} ORDER BY {order}", identifier(table)),
        });
    }
    let virtual_tables = db.rows(
        "SELECT p.name,s.sql FROM pragma_table_list p
         JOIN sqlite_schema s ON s.name=p.name AND s.type='table'
         WHERE p.schema='main' AND p.type='virtual' ORDER BY p.name",
    )?;
    for (index, row) in virtual_tables.into_iter().enumerate() {
        if super::virtual_table::classify(&row[1]) != Module::Fts5 {
            continue;
        }
        if !row[0]
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        {
            return Err("unexpected FTS5 table name".into());
        }
        let vocab = format!("zsqlite_verify_vocab_{index}");
        db.execute(&format!(
            "CREATE VIRTUAL TABLE IF NOT EXISTS temp.{} USING fts5vocab(main,{},instance)",
            identifier(&vocab),
            identifier(&row[0])
        ))?;
        output.push(Verification {
            table: format!("{}-fts-instance", row[0]),
            sql: format!(
                "SELECT term,doc,col,offset FROM temp.{} ORDER BY term,doc,col,offset",
                identifier(&vocab)
            ),
        });
    }
    Ok(output)
}
