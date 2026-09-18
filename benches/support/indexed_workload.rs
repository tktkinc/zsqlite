//! Deterministic, schema-driven SQL read cases for transcript replay benchmarks.
//! `SQLite` shadow tables are storage implementation details and are excluded.

use super::sqlite::{Connection, Result, Value, quote};
use super::virtual_table::Module;

#[derive(Clone, Debug)]
pub struct IndexedCase {
    pub name: String,
    pub table: String,
    pub access_path: String,
    pub sql: String,
    pub parameters: Vec<Value>,
    pub virtual_index: bool,
}

#[derive(Clone, Debug)]
pub struct Coverage {
    pub table: String,
    pub kind: String,
    pub access_paths: usize,
    pub cases: usize,
    pub note: String,
}

pub struct Workload {
    pub cases: Vec<IndexedCase>,
    pub coverage: Vec<Coverage>,
}

fn identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn value_hash(hash: &mut blake3::Hasher, value: &Value) {
    match value {
        Value::Null => {
            hash.update(&[0]);
        }
        Value::Integer(value) => {
            hash.update(&[1]);
            hash.update(&value.to_le_bytes());
        }
        Value::Real(value) => {
            hash.update(&[2]);
            hash.update(&value.to_bits().to_le_bytes());
        }
        Value::Text(value) => {
            hash.update(&[3]);
            hash.update(value);
        }
        Value::Blob(value) => {
            hash.update(&[4]);
            hash.update(value);
        }
    }
}

fn randomized_order(seed: u64, case: &IndexedCase) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(&seed.to_le_bytes());
    hash.update(case.name.as_bytes());
    for value in &case.parameters {
        value_hash(&mut hash, value);
    }
    *hash.finalize().as_bytes()
}

/// Turn one token returned by `fts5vocab` into a literal one-token FTS5
/// phrase. Parameters bind the query language, not an already escaped token,
/// so punctuation and words such as `AND` must still be quoted. FTS5 escapes
/// a quote inside a phrase by doubling it.
pub(crate) fn fts5_phrase(term: Value) -> Result<Value> {
    let Value::Text(term) = term else {
        return Err("fts5vocab returned a non-text term".into());
    };
    let mut query = Vec::with_capacity(term.len() + 2);
    query.push(b'"');
    for byte in term {
        if byte == b'"' {
            query.push(b'"');
        }
        query.push(byte);
    }
    query.push(b'"');
    Ok(Value::Text(query))
}

fn index_columns(db: &Connection, index: &str) -> Result<Vec<String>> {
    let rows = db.rows(&format!(
        "SELECT name,cid FROM pragma_index_xinfo({}) WHERE key=1 ORDER BY seqno",
        quote(index)
    ))?;
    if rows.is_empty()
        || rows
            .iter()
            .any(|row| row[0].is_empty() || row[1].parse::<i64>().is_ok_and(|cid| cid < 0))
    {
        return Ok(Vec::new());
    }
    Ok(rows.into_iter().map(|row| row[0].clone()).collect())
}

fn sample_population(db: &Connection, from: &str, samples: usize) -> Result<(i64, usize)> {
    if samples == 1 {
        let present = usize::from(
            !db.values(&format!("SELECT 1 FROM {from} LIMIT 1"))?
                .is_empty(),
        );
        return Ok((i64::try_from(present)?, present));
    }
    let count = db.scalar(&format!("SELECT count(*) FROM {from}"))?;
    Ok((count, usize::try_from(count)?.min(samples)))
}

fn add_access_path(
    db: &Connection,
    table: &str,
    index: Option<&str>,
    columns: &[String],
    stable_key: &[String],
    samples: usize,
    output: &mut Vec<IndexedCase>,
) -> Result<usize> {
    if columns.is_empty() {
        return Ok(0);
    }
    let table_sql = identifier(table);
    let columns_sql = columns
        .iter()
        .map(|column| identifier(column))
        .collect::<Vec<_>>();
    let mut order_columns = columns.to_vec();
    for column in stable_key {
        if !order_columns.contains(column) {
            order_columns.push(column.clone());
        }
    }
    let order_sql = order_columns
        .iter()
        .map(|column| identifier(column))
        .collect::<Vec<_>>()
        .join(",");
    let nonnull = columns_sql
        .iter()
        .map(|column| format!("{column} IS NOT NULL"))
        .collect::<Vec<_>>()
        .join(" AND ");
    let indexed_by = index.map_or_else(String::new, |name| {
        format!(" INDEXED BY {}", identifier(name))
    });
    let population = format!("{table_sql}{indexed_by} WHERE {nonnull}");
    let (count, sample_count) = sample_population(db, &population, samples)?;
    if sample_count == 0 {
        return Ok(0);
    }
    let mut offsets = (0..sample_count)
        .map(|position| {
            if sample_count == 1 {
                0
            } else {
                i64::try_from(position).unwrap() * (count - 1)
                    / i64::try_from(sample_count - 1).unwrap()
            }
        })
        .collect::<Vec<_>>();
    offsets.dedup();
    let access_path = index.map_or_else(
        || {
            if columns == ["rowid"] {
                "rowid"
            } else {
                "primary-key"
            }
            .to_owned()
        },
        str::to_owned,
    );
    for (sample, offset) in offsets.into_iter().enumerate() {
        let values = db.values(&format!(
            "SELECT {} FROM {table_sql}{indexed_by} WHERE {nonnull} ORDER BY {} LIMIT 1 OFFSET {offset}",
            columns_sql.join(","),
            columns_sql.join(",")
        ))?;
        let parameters = values.into_iter().next().ok_or("sample row disappeared")?;
        let predicates = columns_sql
            .iter()
            .map(|column| format!("{column}=?"))
            .collect::<Vec<_>>()
            .join(" AND ");
        output.push(IndexedCase {
            name: format!("random/{table}/{access_path}/point-{sample}"),
            table: table.to_owned(),
            access_path: access_path.clone(),
            sql: format!(
                "SELECT * FROM {table_sql}{indexed_by} WHERE {predicates} ORDER BY {order_sql} LIMIT 32"
            ),
            parameters: parameters.clone(),
            virtual_index: false,
        });
        output.push(IndexedCase {
            name: format!("random/{table}/{access_path}/range-{sample}"),
            table: table.to_owned(),
            access_path: access_path.clone(),
            sql: format!(
                "SELECT * FROM {table_sql}{indexed_by} WHERE {}>=? ORDER BY {} LIMIT 32",
                columns_sql[0], order_sql
            ),
            parameters: vec![parameters[0].clone()],
            virtual_index: false,
        });
    }
    Ok(sample_count * 2)
}

/// Build point and short range reads for each usable ordinary-table access
/// path, plus token searches for FTS5 virtual tables. NULL-only keys,
/// expression/partial indexes, and empty tables remain visible in `coverage`
/// instead of being silently represented by invented values.
#[allow(clippy::too_many_lines)] // Schema discovery keeps skip accounting beside each decision.
pub fn generate(
    db: &Connection,
    seed: u64,
    samples_per_index: usize,
    allowed_tables: Option<&std::collections::BTreeSet<String>>,
) -> Result<Workload> {
    let tables = db.rows(
        "SELECT p.name,p.type,s.sql FROM pragma_table_list p
         JOIN sqlite_schema s ON s.name=p.name AND s.type='table'
         WHERE p.schema='main' AND p.name NOT LIKE 'sqlite_%'
           AND p.type IN ('table','virtual') ORDER BY p.name",
    )?;
    let mut cases = Vec::new();
    let mut coverage = Vec::new();
    for row in tables {
        let table = &row[0];
        if allowed_tables.is_some_and(|allowed| !allowed.contains(table)) {
            continue;
        }
        let kind = &row[1];
        let create_sql = &row[2];
        if kind == "virtual" {
            let module = super::virtual_table::classify(create_sql);
            if module != Module::Fts5
                || !table
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
            {
                coverage.push(Coverage {
                    table: table.clone(),
                    kind: kind.clone(),
                    access_paths: 0,
                    cases: 0,
                    note: if module == Module::Fts5Vocab {
                        "derived FTS5 vocabulary; not application records"
                    } else {
                        "unsupported virtual table"
                    }
                    .into(),
                });
                continue;
            }
            let vocab = format!("zsqlite_bench_vocab_{}", coverage.len());
            db.execute(&format!(
                "CREATE VIRTUAL TABLE IF NOT EXISTS temp.{} USING fts5vocab(main,{},row)",
                identifier(&vocab),
                identifier(table)
            ))?;
            let terms = db.values(&format!(
                "SELECT term FROM temp.{} ORDER BY term LIMIT {}",
                identifier(&vocab),
                samples_per_index.max(1)
            ))?;
            let count = terms.len();
            for (sample, mut values) in terms.into_iter().enumerate() {
                cases.push(IndexedCase {
                    name: format!("random/{table}/fts/term-{sample}"),
                    table: table.clone(),
                    access_path: "fts5".into(),
                    sql: format!(
                        "SELECT rowid FROM {} WHERE {} MATCH ? ORDER BY rowid LIMIT 32",
                        identifier(table),
                        identifier(table)
                    ),
                    parameters: vec![fts5_phrase(values.remove(0))?],
                    virtual_index: true,
                });
            }
            coverage.push(Coverage {
                table: table.clone(),
                kind: kind.clone(),
                access_paths: usize::from(count > 0),
                cases: count,
                note: if count == 0 {
                    "empty FTS index"
                } else {
                    "FTS5 terms"
                }
                .into(),
            });
            continue;
        }

        let columns = db.rows(&format!(
            "SELECT name,pk FROM pragma_table_xinfo({}) WHERE hidden=0 ORDER BY pk,cid",
            quote(table)
        ))?;
        let mut declared_primary = columns
            .iter()
            .filter(|row| row[1].parse::<i64>().is_ok_and(|position| position > 0))
            .map(|row| (row[1].parse::<i64>().unwrap(), row[0].clone()))
            .collect::<Vec<_>>();
        declared_primary.sort_by_key(|(position, _)| *position);
        let without_rowid = create_sql.to_ascii_uppercase().contains("WITHOUT ROWID");
        let declared_primary = declared_primary
            .into_iter()
            .map(|(_, name)| name)
            .collect::<Vec<_>>();
        let stable_key = if without_rowid {
            declared_primary.clone()
        } else {
            vec!["rowid".into()]
        };
        let mut access_paths = 0;
        let mut generated = 0;
        if !without_rowid {
            let count = add_access_path(
                db,
                table,
                None,
                &["rowid".into()],
                &stable_key,
                samples_per_index,
                &mut cases,
            )?;
            generated += count;
            access_paths += usize::from(count > 0);
        }
        if !declared_primary.is_empty() {
            let count = add_access_path(
                db,
                table,
                None,
                &declared_primary,
                &stable_key,
                samples_per_index,
                &mut cases,
            )?;
            generated += count;
            access_paths += usize::from(count > 0);
        }
        let indexes = db.rows(&format!(
            "SELECT name,partial FROM pragma_index_list({}) ORDER BY name",
            quote(table)
        ))?;
        let mut skipped = 0;
        for index in indexes {
            if index[1] != "0" {
                skipped += 1;
                continue;
            }
            let key = index_columns(db, &index[0])?;
            if key.is_empty() || key == declared_primary {
                skipped += 1;
                continue;
            }
            let count = add_access_path(
                db,
                table,
                Some(&index[0]),
                &key,
                &stable_key,
                samples_per_index,
                &mut cases,
            )?;
            generated += count;
            access_paths += usize::from(count > 0);
            skipped += usize::from(count == 0);
        }
        coverage.push(Coverage {
            table: table.clone(),
            kind: kind.clone(),
            access_paths,
            cases: generated,
            note: if generated == 0 {
                "empty or all usable keys are NULL".into()
            } else if skipped > 0 {
                format!("{skipped} empty / duplicate / partial / expression indexes skipped")
            } else {
                String::new()
            },
        });
    }
    cases.sort_by_key(|case| randomized_order(seed, case));
    Ok(Workload { cases, coverage })
}

pub fn validate_plan(db: &Connection, case: &IndexedCase) -> Result<Vec<String>> {
    let rows = db.query_params(
        &format!("EXPLAIN QUERY PLAN {}", case.sql),
        &case.parameters,
    )?;
    let details = rows
        .into_iter()
        .filter_map(|row| {
            row.last()
                .map(|(_, bytes)| String::from_utf8_lossy(bytes).into_owned())
        })
        .collect::<Vec<_>>();
    let normalized = details.join(" ").to_ascii_uppercase();
    let indexed = if case.virtual_index {
        normalized.contains("VIRTUAL TABLE INDEX")
    } else {
        normalized.contains("SEARCH ")
            && (case.access_path == "rowid"
                || case.access_path == "primary-key"
                || normalized.contains(&case.access_path.to_ascii_uppercase())
                || normalized.contains("PRIMARY KEY"))
    };
    if !indexed {
        return Err(format!(
            "indexed case devolved to a scan: {} table={} via {}: {details:?}",
            case.name, case.table, case.access_path
        )
        .into());
    }
    Ok(details)
}
