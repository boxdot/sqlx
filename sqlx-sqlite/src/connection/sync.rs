use core::fmt::Debug;
use core::iter;

use sqlx_core::database::Database;
use sqlx_core::describe::Describe;
use sqlx_core::executor::Execute;
use sqlx_core::sync_executor::{BoxIter, SyncExecutor};
use sqlx_core::{Either, Error};

use crate::connection::establish::EstablishParams;
use crate::connection::execute;
use crate::{
    Sqlite, SqliteArguments, SqliteConnectOptions, SqliteQueryResult, SqliteRow, SqliteStatement,
};

use super::{describe, worker, ConnectionState};

pub struct SyncSqliteConnection {
    conn: ConnectionState,
}

impl SyncSqliteConnection {
    pub fn establish(options: &SqliteConnectOptions) -> Result<Self, Error> {
        let params = EstablishParams::from_options(options)?;
        let conn = params.establish()?;
        Ok(Self { conn })
    }

    fn execute<'a>(
        &'a mut self,
        query: &'a str,
        args: Option<SqliteArguments<'a>>,
        persistent: bool,
        limit: Option<usize>,
    ) -> Result<impl Iterator<Item = Result<Either<SqliteQueryResult, SqliteRow>, Error>> + 'a, Error>
    {
        let iter = execute::iter(&mut self.conn, query, args, persistent)?;
        if let Some(limit) = limit {
            let idx = 0;
            let iter = iter
                .map(move |item| match item {
                    Ok(item) if item.is_right() => (idx + 1, Ok(item)),
                    _ => (idx, item),
                })
                .take_while(move |(idx, _)| *idx < limit)
                .map(|(_, item)| item);
            Ok(Either::Left(iter))
        } else {
            Ok(Either::Right(iter))
        }
    }

    fn prepare(&mut self, query: &str) -> Result<SqliteStatement<'static>, Error> {
        worker::prepare(&mut self.conn, query)
    }

    fn describe(&mut self, query: &str) -> Result<Describe<Sqlite>, Error> {
        describe::describe(&mut self.conn, query)
    }
}

impl Debug for SyncSqliteConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncSqliteConnection")
            .finish_non_exhaustive()
    }
}

impl<'c> SyncExecutor<'c> for &'c mut SyncSqliteConnection {
    type Database = Sqlite;

    fn fetch_many<'e, 'q: 'e, E>(
        self,
        mut query: E,
    ) -> BoxIter<
        'e,
        Result<
            Either<<Self::Database as Database>::QueryResult, <Self::Database as Database>::Row>,
            Error,
        >,
    >
    where
        'c: 'e,
        E: 'q + Execute<'q, Self::Database>,
    {
        let sql = query.sql();
        let arguments = match query.take_arguments().map_err(Error::Encode) {
            Ok(arguments) => arguments,
            Err(error) => return Box::new(iter::once(Err(error))),
        };
        let persistent = query.persistent() && arguments.is_some();

        Box::new(
            self.execute(sql, arguments, persistent, None)
                .into_iter()
                .flatten(),
        )
    }

    fn fetch_optional<'e, 'q: 'e, E>(
        self,
        mut query: E,
    ) -> Result<Option<<Self::Database as Database>::Row>, Error>
    where
        'c: 'e,
        E: 'q + Execute<'q, Self::Database>,
    {
        let sql = query.sql();
        let arguments = query.take_arguments().map_err(Error::Encode)?;
        let persistent = query.persistent() && arguments.is_some();

        for res in self.execute(sql, arguments, persistent, None)? {
            if let Either::Right(row) = res? {
                return Ok(Some(row));
            }
        }

        Ok(None)
    }

    fn prepare_with<'e, 'q: 'e>(
        self,
        sql: &'q str,
        _parameters: &'e [<Self::Database as Database>::TypeInfo],
    ) -> Result<<Self::Database as Database>::Statement<'q>, Error>
    where
        'c: 'e,
    {
        let statement = self.prepare(sql)?;
        Ok(SqliteStatement {
            sql: sql.into(),
            ..statement
        })
    }

    fn describe<'e, 'q: 'e>(self, sql: &'q str) -> Result<Describe<Self::Database>, Error>
    where
        'c: 'e,
    {
        self.describe(sql)
    }
}
