import logging

import mbdata.models
import sqlalchemy as sa
from sqlalchemy import orm

from .constants import DEFAULT_DB_URI

_engine = None
_Session = None

_logger = logging.getLogger(__name__)


def configure_database(db_url: str = None, echo_sql: bool = False) -> None:
    """Configure the Postgresql database for Musicbrainz

    :param db_url: URI for Postgresql database
    :param echo_sql: Echo all SQL statements to stdout
    """
    global _engine, _Session
    # Create a database connection
    if db_url is not None:
        _logger.debug(f"Using database at custom URI '{db_url}'.")
    else:
        raise Exception("No database file or url provided.")

    _logger.debug(f"Opening database as {db_url}")
    _engine = sa.create_engine(db_url, echo=echo_sql)
    _Session = orm.sessionmaker(_engine)

    mbdata.models.Base.metadata.create_all(_engine)


def get_db_session() -> None:
    """Return a SQLAlchemy session object that can be used to query the database"""
    global _engine
    if _engine is None or _Session is None:
        configure_database(DEFAULT_DB_URI)

    return _Session()
