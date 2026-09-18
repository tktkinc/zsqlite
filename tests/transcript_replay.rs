#![cfg(feature = "static")]
#[allow(dead_code)]
#[path = "../benches/support/indexed_workload.rs"]
mod indexed_workload;
#[path = "../benches/support/read_stream.rs"]
mod read_stream;
#[allow(dead_code)] // The benchmark also uses URI, resource and statistics helpers.
#[path = "../benches/support/replay_sqlite.rs"]
mod sqlite;
#[allow(dead_code)]
#[path = "../benches/support/transcript_schema.rs"]
mod transcript_schema;
#[allow(dead_code)]
#[path = "../benches/support/virtual_table.rs"]
mod virtual_table;
use sqlite::{Connection, Result};

#[test]
fn final_read_connections_scale_with_phases_and_rounds_not_cases() {
    let names = (0..480)
        .map(|index| format!("case-{index}"))
        .collect::<Vec<_>>();
    let names = names.iter().map(String::as_str).collect::<Vec<_>>();
    let phases = [
        "history",
        "before-rollup",
        "after-trained-rollup",
        "after-rollup",
    ];
    let mut connections = 0;
    let mut measurements = 0;
    for phase in phases {
        let mut prior = None;
        for round in 0..read_stream::DEFAULT_ROUNDS {
            let order = read_stream::order(&names, phase, round);
            assert_eq!(order.len(), names.len());
            let mut sorted = order.clone();
            sorted.sort_unstable();
            assert_eq!(sorted, (0..names.len()).collect::<Vec<_>>());
            assert_ne!(prior.as_ref(), Some(&order));
            prior = Some(order);
            connections += 1;
            measurements += names.len() * 2;
        }
    }
    assert_eq!(connections, phases.len() * read_stream::DEFAULT_ROUNDS);
    assert_eq!(connections, 12);
    assert_eq!(measurements, 11_520);
    assert_eq!(
        phases.len() * read_stream::DEFAULT_ROUNDS * names.len(),
        5_760
    );
}

#[test]
fn indexed_workload_covers_tables_indexes_fts_and_skips_shadows() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("indexed.sqlite");
    let db = Connection::open(&path, false, false)?;
    db.execute(
        "CREATE TABLE parent(id INTEGER PRIMARY KEY, code TEXT UNIQUE, payload BLOB);
         CREATE TABLE child(a INTEGER NOT NULL,b INTEGER NOT NULL,value TEXT,
             PRIMARY KEY(a,b)) WITHOUT ROWID;
         CREATE INDEX child_value ON child(value,a);
         CREATE TABLE empty(id INTEGER PRIMARY KEY, value TEXT);
         CREATE VIRTUAL TABLE search USING fts5(body);
         INSERT INTO parent VALUES(1,'one',x'01'),(2,'two',x'02'),(3,'three',x'03');
         INSERT INTO child VALUES(1,1,'alpha'),(1,2,'beta'),(2,1,NULL);
         INSERT INTO search(rowid,body) VALUES(1,'sqlite storage'),(2,'indexed transcript');",
    )?;
    let first = indexed_workload::generate(&db, 42, 2, None)?;
    let second = indexed_workload::generate(&db, 42, 2, None)?;
    assert_eq!(
        first
            .cases
            .iter()
            .map(|case| &case.name)
            .collect::<Vec<_>>(),
        second
            .cases
            .iter()
            .map(|case| &case.name)
            .collect::<Vec<_>>()
    );
    for table in ["parent", "child", "empty", "search"] {
        assert!(first.coverage.iter().any(|entry| entry.table == table));
    }
    for shadow in [
        "search_data",
        "search_idx",
        "search_content",
        "search_docsize",
        "search_config",
    ] {
        assert!(!first.coverage.iter().any(|entry| entry.table == shadow));
    }
    assert!(
        first
            .cases
            .iter()
            .any(|case| case.access_path == "child_value")
    );
    assert!(
        first
            .cases
            .iter()
            .any(|case| { case.table == "parent" && case.access_path == "rowid" })
    );
    assert!(
        first
            .cases
            .iter()
            .any(|case| { case.table == "parent" && case.access_path == "primary-key" })
    );
    assert!(first.cases.iter().any(|case| case.virtual_index));
    for case in &first.cases {
        indexed_workload::validate_plan(&db, case)?;
        let _fingerprint = db.fingerprint_params(&case.sql, &case.parameters)?;
    }
    let empty = first
        .coverage
        .iter()
        .find(|entry| entry.table == "empty")
        .unwrap();
    assert_eq!(empty.cases, 0);
    Ok(())
}

#[test]
fn fts5vocab_is_derived_metadata_not_a_recursive_fts_target() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let db = Connection::open(&directory.path().join("vocab.sqlite"), false, false)?;
    db.execute(
        "CREATE VIRTUAL TABLE conversation_segment_fts USING fts5(asked,outcome);
         CREATE VIRTUAL TABLE conversation_segment_terms
           USING fts5vocab(conversation_segment_fts,'row');
         CREATE VIRTUAL TABLE punctuation_fts USING fts5(
           body,tokenize='unicode61 tokenchars ''#\"+-*:()''');
         INSERT INTO conversation_segment_fts(rowid,asked,outcome)
           VALUES(1,'sqlite index','verified');
         INSERT INTO punctuation_fts(rowid,body)
           VALUES(1,'# \" AND OR NOT +-*:()');",
    )?;
    assert_eq!(
        virtual_table::classify(
            "CREATE VIRTUAL TABLE conversation_segment_terms USING fts5vocab(conversation_segment_fts,'row')"
        ),
        virtual_table::Module::Fts5Vocab
    );
    assert_eq!(
        virtual_table::classify("CREATE VIRTUAL TABLE \"using\" USING fts5(body)"),
        virtual_table::Module::Fts5
    );
    let workload = indexed_workload::generate(&db, 42, 16, None)?;
    assert!(
        workload
            .cases
            .iter()
            .any(|case| case.table == "conversation_segment_fts")
    );
    assert!(
        !workload
            .cases
            .iter()
            .any(|case| case.table == "conversation_segment_terms")
    );
    let terms = workload
        .coverage
        .iter()
        .find(|entry| entry.table == "conversation_segment_terms")
        .unwrap();
    assert_eq!(terms.cases, 0);
    assert_eq!(
        terms.note,
        "derived FTS5 vocabulary; not application records"
    );
    let mut punctuation_queries = std::collections::BTreeSet::new();
    for case in workload.cases.iter().filter(|case| case.virtual_index) {
        indexed_workload::validate_plan(&db, case)?;
        let result = db.fingerprint_params(&case.sql, &case.parameters)?;
        assert!(result.rows > 0);
        if case.table == "punctuation_fts" {
            let sqlite::Value::Text(query) = &case.parameters[0] else {
                panic!("FTS5 query is not text")
            };
            punctuation_queries.insert(String::from_utf8(query.clone())?);
        }
    }
    for query in [
        "\"#\"",
        "\"\"\"\"",
        "\"and\"",
        "\"or\"",
        "\"not\"",
        "\"+-*:()\"",
    ] {
        assert!(punctuation_queries.contains(query), "missing {query:?}");
    }
    let verification = transcript_schema::verification_queries(&db)?;
    assert!(
        verification
            .iter()
            .any(|query| query.table == "conversation_segment_fts-fts-instance")
    );
    assert!(
        !verification
            .iter()
            .any(|query| query.table == "conversation_segment_terms-fts-instance")
    );
    for query in verification {
        let _fingerprint = db.fingerprint(&query.sql)?;
    }
    Ok(())
}

#[test]
fn current_transcript_mutations_churn_indexes_and_restore_typed_rows() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("current.sqlite");
    let db = Connection::open(&path, false, false)?;
    db.execute(
        "PRAGMA foreign_keys=ON; PRAGMA user_version=1;
         CREATE TABLE message(id TEXT PRIMARY KEY);
         CREATE TABLE message_part(id INTEGER PRIMARY KEY,text TEXT);
         CREATE VIRTUAL TABLE message_fts USING fts5(text);
         CREATE TRIGGER message_fts_update AFTER UPDATE OF text ON message_part BEGIN
           DELETE FROM message_fts WHERE rowid=old.id;
           INSERT INTO message_fts(rowid,text) VALUES(new.id,new.text);
         END;
         CREATE TABLE session(id INTEGER PRIMARY KEY,updated_at_ms INTEGER,deleted INTEGER NOT NULL);
         CREATE INDEX session_visible_updated ON session(updated_at_ms DESC,id DESC) WHERE deleted=0;
         CREATE TABLE session_event(id INTEGER PRIMARY KEY,session INTEGER,at_ms INTEGER,source INTEGER,external_id TEXT);
         CREATE INDEX session_event_time ON session_event(session,at_ms,id);
         CREATE UNIQUE INDEX session_event_external ON session_event(source,external_id) WHERE external_id IS NOT NULL;
         CREATE TABLE agent_work(id TEXT PRIMARY KEY,graph TEXT,state INTEGER,parent_session TEXT,child_session TEXT,updated_at INTEGER);
         CREATE INDEX agent_work_graph_state_updated ON agent_work(graph,state,updated_at DESC);
         CREATE INDEX agent_work_parent_state_updated ON agent_work(parent_session,state,updated_at DESC);
         CREATE INDEX agent_work_child_updated ON agent_work(child_session,updated_at DESC);
         CREATE TABLE session_entry(id TEXT PRIMARY KEY,origin TEXT);
         CREATE INDEX session_entry_origin ON session_entry(origin) WHERE origin IS NOT NULL;
         CREATE TABLE tool_call(id TEXT PRIMARY KEY,session INTEGER,created_at_ms INTEGER);
         CREATE INDEX tool_call_session_time ON tool_call(session,created_at_ms,id);
         CREATE TABLE tool_attempt(id TEXT PRIMARY KEY,started_at_ms INTEGER);
         CREATE INDEX tool_attempt_time ON tool_attempt(started_at_ms,id);
         CREATE TABLE conversation_segment(id INTEGER PRIMARY KEY,status TEXT,as_of_ms INTEGER);
         CREATE INDEX conversation_segment_status ON conversation_segment(status,as_of_ms DESC,id);
         CREATE TABLE conversation_segment_entity(id INTEGER PRIMARY KEY,segment INTEGER,entity TEXT,canonical TEXT,UNIQUE(segment,canonical));
         CREATE INDEX conversation_segment_entity_canonical ON conversation_segment_entity(canonical,segment,id);
         CREATE VIRTUAL TABLE conversation_segment_entity_fts USING fts5(entity);
         CREATE VIRTUAL TABLE conversation_segment_entity_trigram USING fts5(entity,tokenize='trigram');
         CREATE TRIGGER conversation_segment_entity_fts_update AFTER UPDATE OF entity ON conversation_segment_entity BEGIN
           DELETE FROM conversation_segment_entity_fts WHERE rowid=old.id;
           DELETE FROM conversation_segment_entity_trigram WHERE rowid=old.id;
           INSERT INTO conversation_segment_entity_fts(rowid,entity) VALUES(new.id,new.entity);
           INSERT INTO conversation_segment_entity_trigram(rowid,entity) VALUES(new.id,new.entity);
         END;
         INSERT INTO message VALUES('m1'),('m2');
         INSERT INTO message_part VALUES(1,'sqlite one'),(2,'sqlite two');
         INSERT INTO message_fts(rowid,text) SELECT id,text FROM message_part;
         INSERT INTO session VALUES(1,100,0),(2,200,0);
         INSERT INTO session_event VALUES(1,1,1000,1,'e1'),(2,2,2000,1,'e2');
         INSERT INTO agent_work VALUES('a1','g',1,'p','c',10),('a2','g',2,'p','c',20);
         INSERT INTO session_entry VALUES('s1','native'),('s2','imported');
         INSERT INTO tool_call VALUES('t1',1,100),('t2',2,200);
         INSERT INTO tool_attempt VALUES('ta1',NULL),('ta2',NULL);
         INSERT INTO conversation_segment VALUES(1,'open',100),(2,'resolved',200);
         INSERT INTO conversation_segment_entity VALUES(1,1,'SQLite','sqlite'),(2,2,'Storage','storage');
         INSERT INTO conversation_segment_entity_fts(rowid,entity) SELECT id,entity FROM conversation_segment_entity;
         INSERT INTO conversation_segment_entity_trigram(rowid,entity) SELECT id,entity FROM conversation_segment_entity;",
    )?;
    assert!(transcript_schema::is_current(&db)?);
    assert_eq!(transcript_schema::preflight(&db)?.len(), 11);
    db.execute("PRAGMA user_version=2")?;
    assert!(!transcript_schema::is_current(&db)?);
    db.execute("PRAGMA user_version=1")?;
    let plan = transcript_schema::MutationPlan::build(&db, 2)?;
    assert_eq!(plan.rows(), 22);
    let message_fts = db.fingerprint("SELECT rowid,text FROM message_fts ORDER BY rowid")?;
    let entity_fts =
        db.fingerprint("SELECT rowid,entity FROM conversation_segment_entity_fts ORDER BY rowid")?;
    plan.apply(&db, 0)?;
    assert!(plan.verify_restored(&db).is_err());
    plan.apply(&db, 1)?;
    plan.verify_restored(&db)?;
    assert_eq!(
        message_fts,
        db.fingerprint("SELECT rowid,text FROM message_fts ORDER BY rowid")?
    );
    assert_eq!(
        entity_fts,
        db.fingerprint("SELECT rowid,entity FROM conversation_segment_entity_fts ORDER BY rowid")?
    );
    let verification = transcript_schema::verification_queries(&db)?;
    assert!(verification.iter().any(|query| query.table == "agent_work"));
    assert!(
        !verification
            .iter()
            .any(|query| query.table.ends_with("_data"))
    );
    assert!(db.rows("PRAGMA foreign_key_check")?.is_empty());
    Ok(())
}

#[test]
fn replay_fingerprints_preserve_types_and_immutable_paths_are_escaped() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("quoted ' ?#%.sqlite");
    let db = Connection::open(&path, false, false)?;
    db.execute("CREATE TABLE t(v); INSERT INTO t VALUES(NULL),(''),(x''),(42),('42'),('日本語');")?;
    let mut hashes = std::collections::BTreeSet::new();
    for rowid in 1..=6 {
        hashes.insert(
            db.fingerprint(&format!("SELECT v FROM t WHERE rowid={rowid}"))?
                .hash
                .to_hex()
                .to_string(),
        );
    }
    assert_eq!(hashes.len(), 6);
    let rows = db.query("SELECT * FROM t ORDER BY rowid")?;
    assert_eq!(rows.len(), 6);
    let first = sqlite::fingerprint_rows(&rows);
    assert_eq!(
        first,
        sqlite::fingerprint_rows(&db.query("SELECT * FROM t ORDER BY rowid")?)
    );
    assert!(db.query("invalid SQL").is_err());
    assert!(db.execute("invalid SQL").is_err());
    let parameters = vec![
        sqlite::Value::Null,
        sqlite::Value::Integer(42),
        sqlite::Value::Real(1.5),
        sqlite::Value::Text(b"text".to_vec()),
        sqlite::Value::Blob(vec![0, 1, 2]),
    ];
    let parameter_path = directory.path().join("query.params");
    sqlite::write_values(&parameter_path, &parameters)?;
    let decoded = sqlite::read_values(&parameter_path)?;
    assert_eq!(format!("{parameters:?}"), format!("{decoded:?}"));
    assert_eq!(
        db.fingerprint_params("SELECT ?,?,?,?,?", &parameters)?,
        db.fingerprint_params("SELECT ?,?,?,?,?", &decoded)?
    );
    let expected = db.fingerprint("SELECT * FROM t ORDER BY rowid")?;
    drop(db);
    let immutable = Connection::open(&path, false, true)?;
    assert_eq!(
        immutable.fingerprint("SELECT * FROM t ORDER BY rowid")?,
        expected
    );
    assert!(immutable.execute("DELETE FROM t").is_err());
    let attached = Connection::open(&directory.path().join("attachment.sqlite"), false, false)?;
    attached.execute(&format!(
        "ATTACH DATABASE {} AS source",
        sqlite::quote(&sqlite::uri(&path))
    ))?;
    assert_eq!(attached.scalar("SELECT count(*) FROM source.t")?, 6);
    Ok(())
}

#[test]
fn replay_adapter_checks_incremental_wal_fts_seals_and_export() -> Result<()> {
    zsqlite::register_static_vfs().map_err(|code| format!("VFS registration: {code}"))?;
    let directory = tempfile::tempdir()?;
    let native = directory.path().join("native.sqlite");
    let managed = directory.path().join("managed.zsqlite");
    for (path, managed) in [(&native, false), (&managed, true)] {
        let db = Connection::open(path, managed, false)?;
        db.execute(
            "PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;
            CREATE TABLE rec(id INTEGER PRIMARY KEY, body TEXT);
            CREATE VIRTUAL TABLE ft USING fts5(t,content='',contentless_delete=1);",
        )?;
        db.checkpoint()?;
    }
    zsqlite::configure(
        &managed,
        zsqlite::StoragePolicy::default()
            .with_dictionary(zsqlite::DictionaryPolicy::new(0, 1024 * 1024)?)
            .with_layout(
                zsqlite::layout::LayoutPolicy::default()
                    .fixed(zsqlite::domain::DecodedBytes::new(64 * 1024))?,
            ),
    )?;
    for id in 1..=10 {
        let mut expected = None;
        for (path, managed) in [(&native, false), (&managed, true)] {
            let db = Connection::open(path, managed, false)?;
            db.execute(&format!(
                "BEGIN; INSERT INTO rec VALUES({id},'sqlite {id}');
                INSERT INTO ft(rowid,t) VALUES({id},'sqlite {id}'); COMMIT;"
            ))?;
            db.checkpoint()?;
            let result = db.fingerprint("SELECT r.id,r.body FROM ft JOIN rec r ON r.id=ft.rowid WHERE ft MATCH 'sqlite' ORDER BY rank,r.id")?;
            if managed {
                assert_eq!(Some(result), expected);
            } else {
                expected = Some(result);
            }
            let _stats = db.stats(managed)?;
        }
        zsqlite::flush(&managed)?;
    }
    zsqlite::compact(&managed)?;
    zsqlite::verify(&managed)?;
    let export = directory.path().join("export.sqlite");
    zsqlite::export_to_sqlite(&managed, &export)?;
    let native = Connection::open(&native, false, true)?;
    let exported = Connection::open(&export, false, true)?;
    assert_eq!(
        native.fingerprint("SELECT * FROM rec ORDER BY id")?,
        exported.fingerprint("SELECT * FROM rec ORDER BY id")?
    );
    Ok(())
}
