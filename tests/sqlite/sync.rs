use std::env;

use anyhow::Context;
use sqlx::{query, Row};
use sqlx_core::sync_executor::SyncExecutor;
use sqlx_sqlite::SyncSqliteConnection;
use sqlx_test::setup_if_needed;

// Make a new sync sqlite connection
//
// Ensure [dotenvy] and [env_logger] have been setup
fn new_sync_sqlite() -> anyhow::Result<SyncSqliteConnection> {
    setup_if_needed();

    let db_url = env::var("DATABASE_URL")?;
    let opts = db_url
        .parse()
        .with_context(|| format!("failed to parse DATABASE_URL={db_url}"))?;
    Ok(SyncSqliteConnection::establish(&opts)?)
}

#[test]
fn it_fetches_and_inflates_row_sync() -> anyhow::Result<()> {
    let mut conn = new_sync_sqlite()?;

    // process rows, one-at-a-time
    // this reuses the memory of the row

    {
        let expected = [15, 39, 51];
        let s = conn.fetch("SELECT 15 UNION SELECT 51 UNION SELECT 39");

        for (i, row) in s.enumerate() {
            let v1 = row?.get::<i32, _>(0);
            assert_eq!(expected[i], v1);
        }
    }

    // same query, but fetch all rows at once
    // this triggers the internal inflation

    let rows = conn.fetch_all("SELECT 15 UNION SELECT 51 UNION SELECT 39")?;

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get::<i32, _>(0), 15);
    assert_eq!(rows[1].get::<i32, _>(0), 39);
    assert_eq!(rows[2].get::<i32, _>(0), 51);

    // same query but fetch the first row a few times from a non-persistent query
    // these rows should be immediately inflated

    let row1 = conn.fetch_one("SELECT 15 UNION SELECT 51 UNION SELECT 39")?;

    assert_eq!(row1.get::<i32, _>(0), 15);

    let row2 = conn.fetch_one("SELECT 15 UNION SELECT 51 UNION SELECT 39")?;

    assert_eq!(row1.get::<i32, _>(0), 15);
    assert_eq!(row2.get::<i32, _>(0), 15);

    // same query (again) but make it persistent
    // and fetch the first row a few times

    let row1 = conn.fetch_one(query("SELECT 15 UNION SELECT 51 UNION SELECT 39"))?;

    assert_eq!(row1.get::<i32, _>(0), 15);

    let row2 = conn.fetch_one(query("SELECT 15 UNION SELECT 51 UNION SELECT 39"))?;

    assert_eq!(row1.get::<i32, _>(0), 15);
    assert_eq!(row2.get::<i32, _>(0), 15);

    Ok(())
}
