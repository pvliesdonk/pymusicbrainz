import logging

import sqlalchemy as sa
import sqlalchemy.orm as orm

_logger = logging.getLogger(__name__)
_engine = None

_Session = None

class Base(orm.DeclarativeBase):
    pass


import mbdata.config

mbdata.config.configure(base_class=Base)
import mbdata.models

_DEFAULT_DB_URI: str = "postgresql://musicbrainz:musicbrainz@musicbrainz.int.liesdonk.nl/musicbrainz_db"


def init_database(db_url: str = None, echo_sql: bool = False):
    global _engine, _Session
    # Create a database connection
    if db_url is not None:
        _logger.debug(f"Using database at custom URI '{db_url}'.")
    else:
        raise Exception("No database file or url provided.")

    _logger.debug(f"Opening database as {db_url}")
    _engine = sa.create_engine(db_url, echo=echo_sql)
    _Session = orm.sessionmaker(_engine)

    Base.metadata.create_all(_engine)


def get_db_session():
    global _engine
    if _engine is None or _Session is None:
        init_database(_DEFAULT_DB_URI)

    return _Session()

