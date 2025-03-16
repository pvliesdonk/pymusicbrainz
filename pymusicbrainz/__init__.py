import pathlib

from . import db

from .search import Search
from .musicbrainz_types import (
    ReleaseType,
    SecondaryTypeList,
    ReleaseStatus,
    PerformanceWorkAttributes,
)
from .identifiers import (
    ArtistID,
    ReleaseGroupID,
    ReleaseID,
    RecordingID,
    TrackID,
    WorkID,
)
from .constants import UNKNOWN_ARTIST_ID, VA_ARTIST_ID
from .factory import MBFactory
from .mbdataclass import Artist, ReleaseGroup, Release, Recording, Medium, Track, Work


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
