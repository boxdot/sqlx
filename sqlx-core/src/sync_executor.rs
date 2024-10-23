use core::fmt::Debug;

use either::Either;

use crate::database::Database;
use crate::describe::Describe;
use crate::executor::Execute;
use crate::Error;

pub type BoxIter<'e, T> = Box<dyn Iterator<Item = T> + 'e>;

pub trait SyncExecutor<'c>: Send + Debug + Sized {
    type Database: Database;

    /// Execute the query and return the total number of rows affected.
    fn execute<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> Result<<Self::Database as Database>::QueryResult, Error>
    where
        'c: 'e,
        E: 'q + Execute<'q, Self::Database>,
    {
        let mut res = <Self::Database as Database>::QueryResult::default();
        for item in self.execute_many(query) {
            res.extend([item?]);
        }
        Ok(res)
    }

    /// Execute multiple queries and return the rows affected from each query, in a stream.
    fn execute_many<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxIter<'e, Result<<Self::Database as Database>::QueryResult, Error>>
    where
        'c: 'e,
        E: 'q + Execute<'q, Self::Database>,
    {
        Box::new(self.fetch_many(query).filter_map(|res| match res {
            Ok(step) => match step {
                Either::Left(rows) => Some(Ok(rows)),
                Either::Right(_) => None,
            },
            Err(err) => Some(Err(err)),
        }))
    }

    /// Execute the query and return the generated results as a stream.
    fn fetch<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxIter<'e, Result<<Self::Database as Database>::Row, Error>>
    where
        'c: 'e,
        E: 'q + Execute<'q, Self::Database>,
    {
        Box::new(self.fetch_many(query).filter_map(|res| match res {
            Ok(step) => match step {
                Either::Left(_) => None,
                Either::Right(row) => Some(Ok(row)),
            },
            Err(e) => Some(Err(e)),
        }))
    }

    /// Execute multiple queries and return the generated results as a stream
    /// from each query, in a stream.
    #[allow(clippy::type_complexity)]
    fn fetch_many<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxIter<
        'e,
        Result<
            Either<<Self::Database as Database>::QueryResult, <Self::Database as Database>::Row>,
            Error,
        >,
    >
    where
        'c: 'e,
        E: 'q + Execute<'q, Self::Database>;

    /// Execute the query and return all the generated results, collected into a [`Vec`].
    fn fetch_all<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> Result<Vec<<Self::Database as Database>::Row>, Error>
    where
        'c: 'e,
        E: 'q + Execute<'q, Self::Database>,
    {
        self.fetch(query).collect()
    }

    /// Execute the query and returns exactly one row.
    fn fetch_one<'e, 'q: 'e, E>(self, query: E) -> Result<<Self::Database as Database>::Row, Error>
    where
        'c: 'e,
        E: 'q + Execute<'q, Self::Database>,
    {
        self.fetch_optional(query)?
            .ok_or_else(|| Error::RowNotFound)
    }

    /// Execute the query and returns at most one row.
    fn fetch_optional<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> Result<Option<<Self::Database as Database>::Row>, Error>
    where
        'c: 'e,
        E: 'q + Execute<'q, Self::Database>;

    /// Prepare the SQL query to inspect the type information of its parameters
    /// and results.
    ///
    /// Be advised that when using the `query`, `query_as`, or `query_scalar` functions, the query
    /// is transparently prepared and executed.
    ///
    /// This explicit API is provided to allow access to the statement metadata available after
    /// it prepared but before the first row is returned.
    #[inline]
    fn prepare<'e, 'q: 'e>(
        self,
        query: &'q str,
    ) -> Result<<Self::Database as Database>::Statement<'q>, Error>
    where
        'c: 'e,
    {
        self.prepare_with(query, &[])
    }

    /// Prepare the SQL query, with parameter type information, to inspect the
    /// type information about its parameters and results.
    ///
    /// Only some database drivers (PostgreSQL, MSSQL) can take advantage of
    /// this extra information to influence parameter type inference.
    fn prepare_with<'e, 'q: 'e>(
        self,
        sql: &'q str,
        parameters: &'e [<Self::Database as Database>::TypeInfo],
    ) -> Result<<Self::Database as Database>::Statement<'q>, Error>
    where
        'c: 'e;

    /// Describe the SQL query and return type information about its parameters
    /// and results.
    ///
    /// This is used by compile-time verification in the query macros to
    /// power their type inference.
    #[doc(hidden)]
    fn describe<'e, 'q: 'e>(self, sql: &'q str) -> Result<Describe<Self::Database>, Error>
    where
        'c: 'e;
}
