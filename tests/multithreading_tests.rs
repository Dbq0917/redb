#[cfg(not(target_os = "wasi"))]
mod multithreading_test {
    use redb::{
        Database, MultimapTableDefinition, ReadableDatabase, ReadableTable, ReadableTableMetadata,
        TableDefinition, TableError,
    };
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;

    fn create_tempfile() -> tempfile::NamedTempFile {
        if cfg!(target_os = "wasi") {
            tempfile::NamedTempFile::new_in("/tmp").unwrap()
        } else {
            tempfile::NamedTempFile::new().unwrap()
        }
    }

    const TABLE: TableDefinition<&str, &str> = TableDefinition::new("x");
    #[test]
    fn len() {
        let tmpfile = create_tempfile();
        let db = Database::create(tmpfile.path()).unwrap();
        let db = Arc::new(db);
        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(TABLE).unwrap();
            table.insert("hello", "world").unwrap();
            table.insert("hello2", "world2").unwrap();
            table.insert("hi", "world").unwrap();
        }
        write_txn.commit().unwrap();

        let db2 = db.clone();
        let t = thread::spawn(move || {
            let read_txn = db2.begin_read().unwrap();
            let table = read_txn.open_table(TABLE).unwrap();
            assert_eq!(table.len().unwrap(), 3);
        });
        t.join().unwrap();

        let read_txn = db.begin_read().unwrap();
        let table = read_txn.open_table(TABLE).unwrap();
        assert_eq!(table.len().unwrap(), 3);
    }

    #[test]
    fn multithreaded_insert() {
        let tmpfile = create_tempfile();
        let db = Database::create(tmpfile.path()).unwrap();

        const DEF1: TableDefinition<&str, &str> = TableDefinition::new("x");
        const DEF2: TableDefinition<&str, &str> = TableDefinition::new("y");
        let write_txn = db.begin_write().unwrap();
        {
            let mut table1 = write_txn.open_table(DEF1).unwrap();
            let mut table2 = write_txn.open_table(DEF2).unwrap();

            thread::scope(|s| {
                s.spawn(|| {
                    table2.insert("hello", "world").unwrap();
                    table2.insert("hello2", "world2").unwrap();
                });
            });

            table1.insert("hello", "world").unwrap();
            table1.insert("hello2", "world2").unwrap();
        }
        write_txn.commit().unwrap();

        let read_txn = db.begin_read().unwrap();
        let table = read_txn.open_table(DEF1).unwrap();
        assert_eq!(table.len().unwrap(), 2);
        let table = read_txn.open_table(DEF2).unwrap();
        assert_eq!(table.len().unwrap(), 2);
    }

    // Stresses concurrent mutation of separate tables within one WriteTransaction: inserts
    // allocate and dirty pages concurrently, and removals of committed entries queue pages on the
    // transaction's shared freed list from multiple threads.
    #[test]
    fn multithreaded_insert_and_remove() {
        let tmpfile = create_tempfile();
        let db = Database::create(tmpfile.path()).unwrap();

        const ELEMENTS: u64 = 1000;
        let table_defs: Vec<TableDefinition<u64, u64>> = vec![
            TableDefinition::new("a"),
            TableDefinition::new("b"),
            TableDefinition::new("c"),
            TableDefinition::new("d"),
        ];

        let write_txn = db.begin_write().unwrap();
        {
            let mut tables: Vec<_> = table_defs
                .iter()
                .map(|def| write_txn.open_table(*def).unwrap())
                .collect();
            thread::scope(|s| {
                for table in tables.iter_mut() {
                    s.spawn(move || {
                        for i in 0..ELEMENTS {
                            table.insert(i, i).unwrap();
                        }
                    });
                }
            });
        }
        write_txn.commit().unwrap();

        // Remove half the now-committed entries and insert new ones, concurrently
        let write_txn = db.begin_write().unwrap();
        {
            let mut tables: Vec<_> = table_defs
                .iter()
                .map(|def| write_txn.open_table(*def).unwrap())
                .collect();
            thread::scope(|s| {
                for table in tables.iter_mut() {
                    s.spawn(move || {
                        for i in 0..ELEMENTS {
                            if i % 2 == 0 {
                                assert_eq!(table.remove(i).unwrap().unwrap().value(), i);
                            } else {
                                table.insert(ELEMENTS + i, i).unwrap();
                            }
                        }
                    });
                }
            });
        }
        write_txn.commit().unwrap();

        let read_txn = db.begin_read().unwrap();
        for def in &table_defs {
            let table = read_txn.open_table(*def).unwrap();
            assert_eq!(table.len().unwrap(), ELEMENTS);
            for i in 0..ELEMENTS {
                if i % 2 == 0 {
                    assert!(table.get(i).unwrap().is_none());
                } else {
                    assert_eq!(table.get(i).unwrap().unwrap().value(), i);
                    assert_eq!(table.get(ELEMENTS + i).unwrap().unwrap().value(), i);
                }
            }
        }
    }

    #[test]
    fn multithreaded_re_read() {
        let tmpfile = create_tempfile();
        let db = Database::create(tmpfile.path()).unwrap();

        const DEF1: TableDefinition<&str, &str> = TableDefinition::new("x");
        const DEF2: TableDefinition<&str, &str> = TableDefinition::new("y");
        const DEF3: TableDefinition<&str, &str> = TableDefinition::new("z");
        let write_txn = db.begin_write().unwrap();
        {
            let mut table1 = write_txn.open_table(DEF1).unwrap();
            let mut table2 = write_txn.open_table(DEF2).unwrap();
            let mut table3 = write_txn.open_table(DEF3).unwrap();
            table1.insert("hello", "world").unwrap();

            thread::scope(|s| {
                s.spawn(|| {
                    let value = table1.get("hello").unwrap().unwrap();
                    table2.insert("hello2", value.value()).unwrap();
                });
            });
            thread::scope(|s| {
                s.spawn(|| {
                    let value = table1.get("hello").unwrap().unwrap();
                    table3.insert("hello2", value.value()).unwrap();
                });
            });

            assert_eq!(table2.get("hello2").unwrap().unwrap().value(), "world");
            assert_eq!(table3.get("hello2").unwrap().unwrap().value(), "world");
        }
        write_txn.commit().unwrap();

        let read_txn = db.begin_read().unwrap();
        let table = read_txn.open_table(DEF1).unwrap();
        assert_eq!(table.len().unwrap(), 1);
        let table = read_txn.open_table(DEF2).unwrap();
        assert_eq!(table.len().unwrap(), 1);
        let table = read_txn.open_table(DEF3).unwrap();
        assert_eq!(table.len().unwrap(), 1);
    }

    #[test]
    fn create_table_is_idempotent_and_reports_reuse() {
        const TABLE: TableDefinition<u64, u64> = TableDefinition::new("shared");
        let tmpfile = create_tempfile();
        let db = Database::create(tmpfile.path()).unwrap();

        let txn = db.begin_write().unwrap();
        let (mut table, created) = txn.create_table(TABLE).unwrap();
        assert!(created);
        table.insert(1, 10).unwrap();
        drop(table);
        txn.commit().unwrap();

        let txn = db.begin_write().unwrap();
        let (table, created) = txn.create_table(TABLE).unwrap();
        assert!(!created);
        assert_eq!(table.get(1).unwrap().unwrap().value(), 10);
        drop(table);
        txn.commit().unwrap();
    }

    #[test]
    fn create_table_keeps_the_stored_type_definition() {
        const TABLE: TableDefinition<u64, u64> = TableDefinition::new("typed");
        let tmpfile = create_tempfile();
        let db = Database::create(tmpfile.path()).unwrap();

        let txn = db.begin_write().unwrap();
        let (mut table, created) = txn.create_table(TABLE).unwrap();
        assert!(created);
        table.insert(1, 10).unwrap();
        drop(table);
        txn.commit().unwrap();

        let wrong_type: TableDefinition<&str, &str> = TableDefinition::new("typed");
        let txn = db.begin_write().unwrap();
        assert!(matches!(
            txn.create_table(wrong_type),
            Err(TableError::TableTypeMismatch(_))
        ));
        drop(txn);

        let txn = db.begin_read().unwrap();
        let table = txn.open_table(TABLE).unwrap();
        assert_eq!(table.get(1).unwrap().unwrap().value(), 10);
    }

    #[test]
    fn create_multimap_table_is_idempotent_and_reports_reuse() {
        const TABLE: MultimapTableDefinition<u64, u64> = MultimapTableDefinition::new("shared");
        let tmpfile = create_tempfile();
        let db = Database::create(tmpfile.path()).unwrap();

        let txn = db.begin_write().unwrap();
        let (mut table, created) = txn.create_multimap_table(TABLE).unwrap();
        assert!(created);
        table.insert(1, 10).unwrap();
        drop(table);
        txn.commit().unwrap();

        let txn = db.begin_write().unwrap();
        let (table, created) = txn.create_multimap_table(TABLE).unwrap();
        assert!(!created);
        assert_eq!(table.len().unwrap(), 1);
        drop(table);
        txn.commit().unwrap();
    }

    #[test]
    fn concurrent_create_of_same_table_reports_one_creation() {
        const TABLE: TableDefinition<u64, u64> = TableDefinition::new("shared_startup");
        let tmpfile = create_tempfile();
        let db = Database::create(tmpfile.path()).unwrap();
        let created = AtomicUsize::new(0);

        thread::scope(|s| {
            let db = &db;
            let created = &created;
            for i in 0..5u64 {
                s.spawn(move || {
                    let txn = db.begin_write().unwrap();
                    let (mut table, was_created) = txn.create_table(TABLE).unwrap();
                    if was_created {
                        created.fetch_add(1, Ordering::Relaxed);
                    }
                    table.insert(i, i * 10).unwrap();
                    drop(table);
                    txn.commit().unwrap();
                });
            }
        });

        assert_eq!(created.load(Ordering::Relaxed), 1);
        let txn = db.begin_read().unwrap();
        let table = txn.open_table(TABLE).unwrap();
        assert_eq!(table.len().unwrap(), 5);
        for i in 0..5u64 {
            assert_eq!(table.get(i).unwrap().unwrap().value(), i * 10);
        }
    }

    #[test]
    fn five_modules_create_their_tables_in_parallel() {
        const TABLES: [TableDefinition<u64, u64>; 5] = [
            TableDefinition::new("module_0"),
            TableDefinition::new("module_1"),
            TableDefinition::new("module_2"),
            TableDefinition::new("module_3"),
            TableDefinition::new("module_4"),
        ];
        let tmpfile = create_tempfile();
        let db = Database::create(tmpfile.path()).unwrap();

        thread::scope(|s| {
            let db = &db;
            for (i, definition) in TABLES.iter().enumerate() {
                let definition = *definition;
                s.spawn(move || {
                    let txn = db.begin_write().unwrap();
                    let (mut table, created) = txn.create_table(definition).unwrap();
                    assert!(created);
                    table.insert(i as u64, i as u64).unwrap();
                    drop(table);
                    txn.commit().unwrap();
                });
            }
        });

        for (i, definition) in TABLES.iter().enumerate() {
            let txn = db.begin_write().unwrap();
            let (table, created) = txn.create_table(*definition).unwrap();
            assert!(!created);
            assert_eq!(table.get(i as u64).unwrap().unwrap().value(), i as u64);
        }
    }
}
