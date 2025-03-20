import pathlib

from pymusicbrainz import db
from pymusicbrainz.identifiers import *
from pymusicbrainz.factory import MBFactory


def get_factory(shelf_file: pathlib.Path = None) -> MBFactory:
    """Get the factory object for Musicbrainz

    :return: Factory object
    """
    return MBFactory.get_factory(shelf_file)


def configure_database(db_url: str = None, echo_sql: bool = False) -> None:
    """Configure the PostgreSQL database for Musicbrainz

    :param db_url: URI for PostgreSQL database
    :param echo_sql: Echo all SQL statements to stdout
    """
    db.configure_database(db_url=db_url, echo_sql=echo_sql)
