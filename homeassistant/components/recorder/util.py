"""SQLAlchemy util functions."""
from __future__ import annotations

from collections.abc import Callable, Generator, Iterable, Sequence
from contextlib import contextmanager
from datetime import date, datetime, timedelta
import functools
from functools import partial
from itertools import islice
import logging
import os
import time
from typing import TYPE_CHECKING, Any, Concatenate, NoReturn, ParamSpec, TypeVar

from awesomeversion import (
    AwesomeVersion,
    AwesomeVersionException,
    AwesomeVersionStrategy,
)
import ciso8601
from sqlalchemy import inspect, text
from sqlalchemy.engine import Result, Row
from sqlalchemy.engine.interfaces import DBAPIConnection
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy.orm.query import Query
from sqlalchemy.orm.session import Session
from sqlalchemy.sql.lambdas import StatementLambdaElement
import voluptuous as vol

from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers import config_validation as cv, issue_registry as ir
import homeassistant.util.dt as dt_util

from .const import DATA_INSTANCE, DOMAIN, SQLITE_URL_PREFIX, SupportedDialect
from .db_schema import (
    TABLE_RECORDER_RUNS,
    TABLE_SCHEMA_CHANGES,
    TABLES_TO_CHECK,
    RecorderRuns,
)
from .models import (
    DatabaseEngine,
    DatabaseOptimizer,
    StatisticPeriod,
    UnsupportedDialect,
    process_timestamp,
)

if TYPE_CHECKING:
    from sqlite3.dbapi2 import Cursor as SQLiteCursor

    from . import Recorder

_RecorderT = TypeVar("_RecorderT", bound="Recorder")
_P = ParamSpec("_P")

_LOGGER = logging.getLogger(__name__)

RETRIES = 3
QUERY_RETRY_WAIT = 0.1
SQLITE3_POSTFIXES = ["", "-wal", "-shm"]
DEFAULT_YIELD_STATES_ROWS = 32768


# Our minimum versions for each database
#
# Older MariaDB suffers https://jira.mariadb.org/browse/MDEV-25020
# which is fixed in 10.5.17, 10.6.9, 10.7.5, 10.8.4
#
def _simple_version(version: str) -> AwesomeVersion:
    """Return a simple version."""
    return AwesomeVersion(version, ensure_strategy=AwesomeVersionStrategy.SIMPLEVER)


MIN_VERSION_MARIA_DB = _simple_version("10.3.0")
RECOMMENDED_MIN_VERSION_MARIA_DB = _simple_version("10.5.17")
MARIADB_WITH_FIXED_IN_QUERIES_105 = _simple_version("10.5.17")
MARIA_DB_106 = _simple_version("10.6.0")
MARIADB_WITH_FIXED_IN_QUERIES_106 = _simple_version("10.6.9")
RECOMMENDED_MIN_VERSION_MARIA_DB_106 = _simple_version("10.6.9")
MARIA_DB_107 = _simple_version("10.7.0")
RECOMMENDED_MIN_VERSION_MARIA_DB_107 = _simple_version("10.7.5")
MARIADB_WITH_FIXED_IN_QUERIES_107 = _simple_version("10.7.5")
MARIA_DB_108 = _simple_version("10.8.0")
RECOMMENDED_MIN_VERSION_MARIA_DB_108 = _simple_version("10.8.4")
MARIADB_WITH_FIXED_IN_QUERIES_108 = _simple_version("10.8.4")
MIN_VERSION_MYSQL = _simple_version("8.0.0")
MIN_VERSION_PGSQL = _simple_version("12.0")
MIN_VERSION_SQLITE = _simple_version("3.31.0")


# This is the maximum time after the recorder ends the session
# before we no longer consider startup to be a "restart" and we
# should do a check on the sqlite3 database.
MAX_RESTART_TIME = timedelta(minutes=10)

# Retry when one of the following MySQL errors occurred:
RETRYABLE_MYSQL_ERRORS = (1205, 1206, 1213)
# 1205: Lock wait timeout exceeded; try restarting transaction
# 1206: The total number of locks exceeds the lock table size
# 1213: Deadlock found when trying to get lock; try restarting transaction

FIRST_POSSIBLE_SUNDAY = 8
SUNDAY_WEEKDAY = 6
DAYS_IN_WEEK = 7


@contextmanager
def session_scope(
    *,
    hass: HomeAssistant | None = None,
    session: Session | None = None,
    exception_filter: Callable[[Exception], bool] | None = None,
    read_only: bool = False,
) -> Generator[Session, None, None]:
    """Provide a transactional scope around a series of operations.

    read_only is used to indicate that the session is only used for reading
    data and that no commit is required. It does not prevent the session
    from writing and is not a security measure.
    """
    if session is None and hass is not None:
        session = get_instance(hass).get_session()

    if session is None:
        raise RuntimeError("Session required")

    need_rollback = False
    try:
        yield session
        if session.get_transaction() and not read_only:
            need_rollback = True
            session.commit()
    except Exception as err:  # pylint: disable=broad-except
        _LOGGER.exception("Error executing query: %s", err)
        if need_rollback:
            session.rollback()
        if not exception_filter or not exception_filter(err):
            raise
    finally:
        session.close()


def log_debug(debug, to_native, result, timer_start):
    elapsed = time.perf_counter() - timer_start
    if to_native:
        _LOGGER.debug(
            "converting %d rows to native objects took %fs",
            len(result),
            elapsed,
        )
    else:
        _LOGGER.debug(
            "querying %d rows took %fs",
            len(result),
            elapsed,
        )

def handle_query_error(err, tryno):
    _LOGGER.error("Error executing query: %s", err)

    if tryno == RETRIES - 1:
        raise
    time.sleep(QUERY_RETRY_WAIT)

def execute_query(qry, to_native, validate_entity_ids):
    if to_native:
        return [
            row
            for row in (
                row.to_native(validate_entity_id=validate_entity_ids)
                for row in qry
            )
            if row is not None
        ]
    else:
        return qry.all()

def execute(
    qry: Query, to_native: bool = False, validate_entity_ids: bool = True
) -> list[Row]:
    """Query the database and convert the objects to HA native form.

    This method also retries a few times in the case of stale connections.
    """
    debug = _LOGGER.isEnabledFor(logging.DEBUG)

    for tryno in range(RETRIES):
        try:
            timer_start = None
            if debug:
                timer_start = time.perf_counter()

            result = execute_query(qry, to_native, validate_entity_ids)

            if debug:
                log_debug(debug, to_native, result, timer_start)

            return result

        except SQLAlchemyError as err:
            handle_query_error(err, tryno)

    # Unreachable
    raise RuntimeError  # pragma: no cover


def execute_stmt_lambda_element(
    session: Session,
    stmt: StatementLambdaElement,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    yield_per: int = DEFAULT_YIELD_STATES_ROWS,
    orm_rows: bool = True,
) -> Sequence[Row] | Result:
    """Execute a StatementLambdaElement.

    If the time window passed is greater than one day
    the execution method will switch to yield_per to
    reduce memory pressure.

    It is not recommended to pass a time window
    when selecting non-ranged rows (ie selecting
    specific entities) since they are usually faster
    with .all().
    """
    use_all = not start_time or ((end_time or dt_util.utcnow()) - start_time).days <= 1
    for tryno in range(RETRIES):
        try:
            if orm_rows:
                executed = session.execute(stmt)
            else:
                executed = session.connection().execute(stmt)
            if use_all:
                return executed.all()
            return executed.yield_per(yield_per)
        except SQLAlchemyError as err:
            _LOGGER.error("Error executing query: %s", err)
            if tryno == RETRIES - 1:
                raise
            time.sleep(QUERY_RETRY_WAIT)

    # Unreachable
    raise RuntimeError  # pragma: no cover


def validate_or_move_away_sqlite_database(dburl: str) -> bool:
    """Ensure that the database is valid or move it away."""
    dbpath = dburl_to_path(dburl)

    if not os.path.exists(dbpath):
        # Database does not exist yet, this is OK
        return True

    if not validate_sqlite_database(dbpath):
        move_away_broken_database(dbpath)
        return False

    return True


def dburl_to_path(dburl: str) -> str:
    """Convert the db url into a filesystem path."""
    return dburl.removeprefix(SQLITE_URL_PREFIX)


def last_run_was_recently_clean(cursor: SQLiteCursor) -> bool:
    """Verify the last recorder run was recently clean."""

    cursor.execute("SELECT end FROM recorder_runs ORDER BY start DESC LIMIT 1;")
    end_time = cursor.fetchone()

    if not end_time or not end_time[0]:
        return False

    last_run_end_time = process_timestamp(dt_util.parse_datetime(end_time[0]))
    assert last_run_end_time is not None
    now = dt_util.utcnow()

    _LOGGER.debug("The last run ended at: %s (now: %s)", last_run_end_time, now)

    if last_run_end_time + MAX_RESTART_TIME < now:
        return False

    return True


def basic_sanity_check(cursor: SQLiteCursor) -> bool:
    """Check tables to make sure select does not fail."""

    for table in TABLES_TO_CHECK:
        if table in (TABLE_RECORDER_RUNS, TABLE_SCHEMA_CHANGES):
            cursor.execute(f"SELECT * FROM {table};")  # noqa: S608 # not injection
        else:
            cursor.execute(
                f"SELECT * FROM {table} LIMIT 1;"  # noqa: S608 # not injection
            )

    return True


def validate_sqlite_database(dbpath: str) -> bool:
    """Run a quick check on an sqlite database to see if it is corrupt."""
    import sqlite3  # pylint: disable=import-outside-toplevel

    try:
        conn = sqlite3.connect(dbpath)
        run_checks_on_open_db(dbpath, conn.cursor())
        conn.close()
    except sqlite3.DatabaseError:
        _LOGGER.exception("The database at %s is corrupt or malformed", dbpath)
        return False

    return True


def run_checks_on_open_db(dbpath: str, cursor: SQLiteCursor) -> None:
    """Run checks that will generate a sqlite3 exception if there is corruption."""
    sanity_check_passed = basic_sanity_check(cursor)
    last_run_was_clean = last_run_was_recently_clean(cursor)

    if sanity_check_passed and last_run_was_clean:
        _LOGGER.debug(
            "The system was restarted cleanly and passed the basic sanity check"
        )
        return

    if not sanity_check_passed:
        _LOGGER.warning(
            "The database sanity check failed to validate the sqlite3 database at %s",
            dbpath,
        )

    if not last_run_was_clean:
        _LOGGER.warning(
            (
                "The system could not validate that the sqlite3 database at %s was"
                " shutdown cleanly"
            ),
            dbpath,
        )


def move_away_broken_database(dbfile: str) -> None:
    """Move away a broken sqlite3 database."""

    isotime = dt_util.utcnow().isoformat()
    corrupt_postfix = f".corrupt.{isotime}"

    _LOGGER.error(
        (
            "The system will rename the corrupt database file %s to %s in order to"
            " allow startup to proceed"
        ),
        dbfile,
        f"{dbfile}{corrupt_postfix}",
    )

    for postfix in SQLITE3_POSTFIXES:
        path = f"{dbfile}{postfix}"
        if not os.path.exists(path):
            continue
        os.rename(path, f"{path}{corrupt_postfix}")


def execute_on_connection(dbapi_connection: DBAPIConnection, statement: str) -> None:
    """Execute a single statement with a dbapi connection."""
    cursor = dbapi_connection.cursor()
    cursor.execute(statement)
    cursor.close()


def query_on_connection(dbapi_connection: DBAPIConnection, statement: str) -> Any:
    """Execute a single statement with a dbapi connection and return the result."""
    cursor = dbapi_connection.cursor()
    cursor.execute(statement)
    result = cursor.fetchall()
    cursor.close()
    return result


def _fail_unsupported_dialect(dialect_name: str) -> NoReturn:
    """Warn about unsupported database version."""
    _LOGGER.error(
        (
            "Database %s is not supported; Home Assistant supports %s. "
            "Starting with Home Assistant 2022.6 this prevents the recorder from "
            "starting. Please migrate your database to a supported software"
        ),
        dialect_name,
        "MariaDB ≥ 10.3, MySQL ≥ 8.0, PostgreSQL ≥ 12, SQLite ≥ 3.31.0",
    )
    raise UnsupportedDialect


def _fail_unsupported_version(
    server_version: str, dialect_name: str, minimum_version: str
) -> NoReturn:
    """Warn about unsupported database version."""
    _LOGGER.error(
        (
            "Version %s of %s is not supported; minimum supported version is %s. "
            "Starting with Home Assistant 2022.6 this prevents the recorder from "
            "starting. Please upgrade your database software"
        ),
        server_version,
        dialect_name,
        minimum_version,
    )
    raise UnsupportedDialect


def _extract_version_from_server_response(
    server_response: str,
) -> AwesomeVersion | None:
    """Attempt to extract version from server response."""
    try:
        return AwesomeVersion(
            server_response,
            ensure_strategy=AwesomeVersionStrategy.SIMPLEVER,
            find_first_match=True,
        )
    except AwesomeVersionException:
        return None


def _datetime_or_none(value: str) -> datetime | None:
    """Fast version of mysqldb DateTime_or_None.

    https://github.com/PyMySQL/mysqlclient/blob/v2.1.0/MySQLdb/times.py#L66
    """
    try:
        return ciso8601.parse_datetime(value)
    except ValueError:
        return None


def build_mysqldb_conv() -> dict:
    """Build a MySQLDB conv dict that uses cisco8601 to parse datetimes."""
    # Late imports since we only call this if they are using mysqldb
    # pylint: disable=import-outside-toplevel
    from MySQLdb.constants import FIELD_TYPE
    from MySQLdb.converters import conversions

    return {**conversions, FIELD_TYPE.DATETIME: _datetime_or_none}


@callback
def _async_create_mariadb_range_index_regression_issue(
    hass: HomeAssistant, version: AwesomeVersion
) -> None:
    """Create an issue for the index range regression in older MariaDB.

    The range scan issue was fixed in MariaDB 10.5.17, 10.6.9, 10.7.5, 10.8.4 and later.
    """
    if version >= MARIA_DB_108:
        min_version = RECOMMENDED_MIN_VERSION_MARIA_DB_108
    elif version >= MARIA_DB_107:
        min_version = RECOMMENDED_MIN_VERSION_MARIA_DB_107
    elif version >= MARIA_DB_106:
        min_version = RECOMMENDED_MIN_VERSION_MARIA_DB_106
    else:
        min_version = RECOMMENDED_MIN_VERSION_MARIA_DB
    ir.async_create_issue(
        hass,
        DOMAIN,
        "maria_db_range_index_regression",
        is_fixable=False,
        severity=ir.IssueSeverity.CRITICAL,
        learn_more_url="https://jira.mariadb.org/browse/MDEV-25020",
        translation_key="maria_db_range_index_regression",
        translation_placeholders={"min_version": str(min_version)},
    )


def handle_sqlite_connection(dbapi_connection, first_connection, instance, version):
    # SQLite-specific connection setup logic
    # ...

def handle_mysql_connection(dbapi_connection, first_connection, instance, version):
    # MySQL-specific connection setup logic
    # ...

def handle_postgresql_connection(dbapi_connection, first_connection, version):
    # PostgreSQL-specific connection setup logic
    # ...

def check_version(version, version_string, db_name, min_version):
    # Version checking logic
    if not version or version < min_version:
        _fail_unsupported_version(
            version or version_string, db_name, min_version
        )

def setup_connection_for_dialect(
    instance: Recorder,
    dialect_name: str,
    dbapi_connection: DBAPIConnection,
    first_connection: bool,
) -> DatabaseEngine | None:
    """Execute statements needed for dialect connection."""
    version: AwesomeVersion | None = None
    slow_range_in_select = False

    if dialect_name == SupportedDialect.SQLITE:
        version = handle_sqlite_connection(dbapi_connection, first_connection, instance, version)
        check_version(version, version_string, "SQLite", MIN_VERSION_SQLITE)

    elif dialect_name == SupportedDialect.MYSQL:
        version = handle_mysql_connection(dbapi_connection, first_connection, instance, version)
        check_version(version, version_string, "MySQL", MIN_VERSION_MYSQL)

    elif dialect_name == SupportedDialect.POSTGRESQL:
        version = handle_postgresql_connection(dbapi_connection, first_connection, version)
        check_version(version, version_string, "PostgreSQL", MIN_VERSION_PGSQL)

    else:
        _fail_unsupported_dialect(dialect_name)

    if not first_connection:
        return None

    return DatabaseEngine(
        dialect=SupportedDialect(dialect_name),
        version=version,
        optimizer=DatabaseOptimizer(slow_range_in_select=slow_range_in_select),
    )


def end_incomplete_runs(session: Session, start_time: datetime) -> None:
    """End any incomplete recorder runs."""
    for run in session.query(RecorderRuns).filter_by(end=None):
        run.closed_incorrect = True
        run.end = start_time
        _LOGGER.warning(
            "Ended unfinished session (id=%s from %s)", run.run_id, run.start
        )
        session.add(run)


def _is_retryable_error(instance: Recorder, err: OperationalError) -> bool:
    """Return True if the error is retryable."""
    assert instance.engine is not None
    return bool(
        instance.engine.dialect.name == SupportedDialect.MYSQL
        and isinstance(err.orig, BaseException)
        and err.orig.args
        and err.orig.args[0] in RETRYABLE_MYSQL_ERRORS
    )


_FuncType = Callable[Concatenate[_RecorderT, _P], bool]


def retryable_database_job(
    description: str,
) -> Callable[[_FuncType[_RecorderT, _P]], _FuncType[_RecorderT, _P]]:
    """Try to execute a database job.

    The job should return True if it finished, and False if it needs to be rescheduled.
    """

    def decorator(job: _FuncType[_RecorderT, _P]) -> _FuncType[_RecorderT, _P]:
        @functools.wraps(job)
        def wrapper(instance: _RecorderT, *args: _P.args, **kwargs: _P.kwargs) -> bool:
            try:
                return job(instance, *args, **kwargs)
            except OperationalError as err:
                if _is_retryable_error(instance, err):
                    assert isinstance(err.orig, BaseException)
                    _LOGGER.info(
                        "%s; %s not completed, retrying", err.orig.args[1], description
                    )
                    time.sleep(instance.db_retry_wait)
                    # Failed with retryable error
                    return False

                _LOGGER.warning("Error executing %s: %s", description, err)

            # Failed with permanent error
            return True

        return wrapper

    return decorator


_WrappedFuncType = Callable[Concatenate[_RecorderT, _P], None]


def database_job_retry_wrapper(
    description: str, attempts: int = 5
) -> Callable[[_WrappedFuncType[_RecorderT, _P]], _WrappedFuncType[_RecorderT, _P]]:
    """Try to execute a database job multiple times.

    This wrapper handles InnoDB deadlocks and lock timeouts.

    This is different from retryable_database_job in that it will retry the job
    attempts number of times instead of returning False if the job fails.
    """

    def decorator(
        job: _WrappedFuncType[_RecorderT, _P]
    ) -> _WrappedFuncType[_RecorderT, _P]:
        @functools.wraps(job)
        def wrapper(instance: _RecorderT, *args: _P.args, **kwargs: _P.kwargs) -> None:
            for attempt in range(attempts):
                try:
                    job(instance, *args, **kwargs)
                    return
                except OperationalError as err:
                    if attempt == attempts - 1 or not _is_retryable_error(
                        instance, err
                    ):
                        raise
                    assert isinstance(err.orig, BaseException)
                    _LOGGER.info(
                        "%s; %s failed, retrying", err.orig.args[1], description
                    )
                    time.sleep(instance.db_retry_wait)
                    # Failed with retryable error

        return wrapper

    return decorator


def periodic_db_cleanups(instance: Recorder) -> None:
    """Run any database cleanups that need to happen periodically.

    These cleanups will happen nightly or after any purge.
    """
    assert instance.engine is not None
    if instance.engine.dialect.name == SupportedDialect.SQLITE:
        # Execute sqlite to create a wal checkpoint and free up disk space
        _LOGGER.debug("WAL checkpoint")
        with instance.engine.connect() as connection:
            connection.execute(text("PRAGMA wal_checkpoint(TRUNCATE);"))
            connection.execute(text("PRAGMA OPTIMIZE;"))


@contextmanager
def write_lock_db_sqlite(instance: Recorder) -> Generator[None, None, None]:
    """Lock database for writes."""
    assert instance.engine is not None
    with instance.engine.connect() as connection:
        # Execute sqlite to create a wal checkpoint
        # This is optional but makes sure the backup is going to be minimal
        connection.execute(text("PRAGMA wal_checkpoint(TRUNCATE)"))
        # Create write lock
        _LOGGER.debug("Lock database")
        connection.execute(text("BEGIN IMMEDIATE;"))
        try:
            yield
        finally:
            _LOGGER.debug("Unlock database")
            connection.execute(text("END;"))


def async_migration_in_progress(hass: HomeAssistant) -> bool:
    """Determine if a migration is in progress.

    This is a thin wrapper that allows us to change
    out the implementation later.
    """
    if DATA_INSTANCE not in hass.data:
        return False
    instance = get_instance(hass)
    return instance.migration_in_progress


def async_migration_is_live(hass: HomeAssistant) -> bool:
    """Determine if a migration is live.

    This is a thin wrapper that allows us to change
    out the implementation later.
    """
    if DATA_INSTANCE not in hass.data:
        return False
    instance: Recorder = hass.data[DATA_INSTANCE]
    return instance.migration_is_live


def second_sunday(year: int, month: int) -> date:
    """Return the datetime.date for the second sunday of a month."""
    second = date(year, month, FIRST_POSSIBLE_SUNDAY)
    day_of_week = second.weekday()
    if day_of_week == SUNDAY_WEEKDAY:
        return second
    return second.replace(
        day=(FIRST_POSSIBLE_SUNDAY + (SUNDAY_WEEKDAY - day_of_week) % DAYS_IN_WEEK)
    )


def is_second_sunday(date_time: datetime) -> bool:
    """Check if a time is the second sunday of the month."""
    return bool(second_sunday(date_time.year, date_time.month).day == date_time.day)


def get_instance(hass: HomeAssistant) -> Recorder:
    """Get the recorder instance."""
    instance: Recorder = hass.data[DATA_INSTANCE]
    return instance


PERIOD_SCHEMA = vol.Schema(
    {
        vol.Exclusive("calendar", "period"): vol.Schema(
            {
                vol.Required("period"): vol.Any("hour", "day", "week", "month", "year"),
                vol.Optional("offset"): int,
            }
        ),
        vol.Exclusive("fixed_period", "period"): vol.Schema(
            {
                vol.Optional("start_time"): vol.All(cv.datetime, dt_util.as_utc),
                vol.Optional("end_time"): vol.All(cv.datetime, dt_util.as_utc),
            }
        ),
        vol.Exclusive("rolling_window", "period"): vol.Schema(
            {
                vol.Required("duration"): cv.time_period_dict,
                vol.Optional("offset"): cv.time_period_dict,
            }
        ),
    }
)


def resolve_period(
    period_def: StatisticPeriod,
) -> tuple[datetime | None, datetime | None]:
    """Return start and end datetimes for a statistic period definition."""
    start_time = None
    end_time = None

    if "calendar" in period_def:
        calendar_period = period_def["calendar"]["period"]
        start_of_day = dt_util.start_of_local_day()
        cal_offset = period_def["calendar"].get("offset", 0)
        if calendar_period == "hour":
            start_time = dt_util.now().replace(minute=0, second=0, microsecond=0)
            start_time += timedelta(hours=cal_offset)
            end_time = start_time + timedelta(hours=1)
        elif calendar_period == "day":
            start_time = start_of_day
            start_time += timedelta(days=cal_offset)
            end_time = start_time + timedelta(days=1)
        elif calendar_period == "week":
            start_time = start_of_day - timedelta(days=start_of_day.weekday())
            start_time += timedelta(days=cal_offset * 7)
            end_time = start_time + timedelta(weeks=1)
        elif calendar_period == "month":
            start_time = start_of_day.replace(day=28)
            # This works for up to 48 months of offset
            start_time = (start_time + timedelta(days=cal_offset * 31)).replace(day=1)
            end_time = (start_time + timedelta(days=31)).replace(day=1)
        else:  # calendar_period = "year"
            start_time = start_of_day.replace(month=12, day=31)
            # This works for 100+ years of offset
            start_time = (start_time + timedelta(days=cal_offset * 366)).replace(
                month=1, day=1
            )
            end_time = (start_time + timedelta(days=365)).replace(day=1)

        start_time = dt_util.as_utc(start_time)
        end_time = dt_util.as_utc(end_time)

    elif "fixed_period" in period_def:
        start_time = period_def["fixed_period"].get("start_time")
        end_time = period_def["fixed_period"].get("end_time")

    elif "rolling_window" in period_def:
        duration = period_def["rolling_window"]["duration"]
        now = dt_util.utcnow()
        start_time = now - duration
        end_time = start_time + duration

        if offset := period_def["rolling_window"].get("offset"):
            start_time += offset
            end_time += offset

    return (start_time, end_time)


def take(take_num: int, iterable: Iterable) -> list[Any]:
    """Return first n items of the iterable as a list.

    From itertools recipes
    """
    return list(islice(iterable, take_num))


def chunked(iterable: Iterable, chunked_num: int) -> Iterable[Any]:
    """Break *iterable* into lists of length *n*.

    From more-itertools
    """
    return iter(partial(take, chunked_num, iter(iterable)), [])


def get_index_by_name(session: Session, table_name: str, index_name: str) -> str | None:
    """Get an index by name."""
    connection = session.connection()
    inspector = inspect(connection)
    indexes = inspector.get_indexes(table_name)
    return next(
        (
            possible_index["name"]
            for possible_index in indexes
            if possible_index["name"]
            and (
                possible_index["name"] == index_name
                or possible_index["name"].endswith(f"_{index_name}")
            )
        ),
        None,
    )
