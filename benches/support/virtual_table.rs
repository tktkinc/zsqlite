//! Categorization of virtual-table modules found in `sqlite_schema.sql`.
//!
//! Matching the module name exactly matters: `fts5vocab` contains the text
//! `fts5`, but it is a derived vocabulary view and cannot itself back another
//! `fts5vocab` table or a `MATCH` workload.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Module {
    Fts5,
    Fts5Vocab,
    Other,
}

fn identifier_end(sql: &[u8], start: usize) -> usize {
    sql[start..]
        .iter()
        .position(|byte| !byte.is_ascii_alphanumeric() && *byte != b'_')
        .map_or(sql.len(), |length| start + length)
}

fn quoted_end(sql: &[u8], start: usize, delimiter: u8) -> usize {
    let closing = if delimiter == b'[' { b']' } else { delimiter };
    let mut cursor = start + 1;
    while cursor < sql.len() {
        if sql[cursor] == closing {
            if closing != b']' && sql.get(cursor + 1) == Some(&closing) {
                cursor += 2;
                continue;
            }
            return cursor;
        }
        cursor += 1;
    }
    sql.len()
}

fn classify_identifier(identifier: &[u8]) -> Module {
    if identifier.eq_ignore_ascii_case(b"fts5") {
        Module::Fts5
    } else if identifier.eq_ignore_ascii_case(b"fts5vocab") {
        Module::Fts5Vocab
    } else {
        Module::Other
    }
}

/// Return the exact module used by a `CREATE VIRTUAL TABLE` statement.
/// Quoted table names and string literals are skipped so a table named
/// `"using"` or an option containing that word cannot confuse discovery.
pub fn classify(create_sql: &str) -> Module {
    let sql = create_sql.as_bytes();
    let mut cursor = 0;
    while cursor < sql.len() {
        match sql[cursor] {
            b'\'' | b'"' | b'`' | b'[' => {
                let end = quoted_end(sql, cursor, sql[cursor]);
                cursor = end.saturating_add(1);
            }
            byte if byte.is_ascii_alphabetic() || byte == b'_' => {
                let end = identifier_end(sql, cursor);
                if sql[cursor..end].eq_ignore_ascii_case(b"using") {
                    cursor = end;
                    while cursor < sql.len() && sql[cursor].is_ascii_whitespace() {
                        cursor += 1;
                    }
                    if cursor == sql.len() {
                        return Module::Other;
                    }
                    return match sql[cursor] {
                        b'"' | b'`' | b'[' => {
                            let end = quoted_end(sql, cursor, sql[cursor]);
                            classify_identifier(&sql[cursor + 1..end])
                        }
                        _ => {
                            let end = identifier_end(sql, cursor);
                            classify_identifier(&sql[cursor..end])
                        }
                    };
                }
                cursor = end;
            }
            _ => cursor += 1,
        }
    }
    Module::Other
}
