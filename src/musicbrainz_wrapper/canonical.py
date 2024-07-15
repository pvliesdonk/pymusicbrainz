import logging
import pathlib
import uuid
from typing import List

import sqlalchemy as sa
from sqlalchemy import orm
from sqlalchemy.ext.hybrid import hybrid_property

from .datatypes import ReleaseID, ReleaseGroupID, RecordingID, ArtistID

_logger = logging.getLogger(__name__)

_engine = None

_DEFAULT_DB_FILE: pathlib.Path = pathlib.Path("mb_canonical_db")

_CANONICAL_DATA_URL: str = "https://data.metabrainz.org/pub/musicbrainz/canonical_data/"

class Base(orm.DeclarativeBase):
    pass


class CanonicalReleaseMapping(Base):
    __tablename__ = 'canonical_release_mapping'

    release_mbid: orm.Mapped[ReleaseID] = orm.mapped_column(sa.types.Uuid, primary_key=True)
    canonical_release_mbid: orm.Mapped[ReleaseID] = orm.mapped_column(sa.types.Uuid)
    release_group_mbid: orm.Mapped[ReleaseGroupID] = orm.mapped_column(sa.types.Uuid)


class CanonicalRecordingMapping(Base):
    __tablename__ = 'canonical_recording_mapping'

    recording_mbid: orm.Mapped[RecordingID] = orm.mapped_column(sa.types.Uuid, primary_key=True)
    canonical_recording_mbid: orm.Mapped[RecordingID] = orm.mapped_column(sa.types.Uuid)
    canonical_release_mbid: orm.Mapped[ReleaseID] = orm.mapped_column(sa.types.Uuid)


class ArtistCredit(Base):
    __tablename__ = 'artist_credit'

    artist_credit_id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    artist_credit_name: orm.Mapped[str] = orm.mapped_column()

    artist_mbids: orm.Mapped[List[ArtistID]] = orm.relationship(
        "ArtistCreditArtist",
        cascade="all", lazy="selectin"
    )


class ArtistCreditArtist(Base):
    __tablename__ = 'artist_credit_artist'
    artist_credit_id: orm.Mapped[int] = orm.mapped_column(sa.ForeignKey("artist_credit.artist_credit_id"), primary_key=True)
    artist_mbid: orm.Mapped[ArtistID] = orm.mapped_column(sa.types.Uuid)



class CanonicalMetadata(Base):
    __tablename__ = 'canonical_metadata'

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    artist_credit_id: orm.Mapped[int] = orm.mapped_column(sa.ForeignKey("artist_credit.artist_credit_id"))
    release_mbid: orm.Mapped[ReleaseID] = orm.mapped_column(sa.types.Uuid)
    release_name: orm.Mapped[str] = orm.mapped_column(index=True)
    recording_mbid: orm.Mapped[RecordingID] = orm.mapped_column(sa.types.Uuid)
    recording_name: orm.Mapped[str] = orm.mapped_column(index=True)
    combined_lookup: orm.Mapped[str] = orm.mapped_column(index=True)
    score: orm.Mapped[int] = orm.mapped_column(index=True)


def init_database(
        db_file: pathlib.Path = None, db_url: str = None, echo_sql: bool = True
):
    global _engine
    # Create a database connection
    if db_url is not None:
        _logger.debug(f"Using database at custom URI '{db_url}'.")
        database_url = db_url
    elif db_file is None:
        raise Exception("No database file or url provided.")
    else:
        _logger.debug(f"Using sqlite3 database in file {db_file.absolute()}")
        database_url = f"sqlite+pysqlite:///{db_file.as_posix()}"

    _logger.debug(f"Opening/creating database as {database_url}")
    _engine = sa.create_engine(database_url, echo=echo_sql)

    Base.metadata.create_all(_engine)


def get_session(db_file: pathlib.Path = _DEFAULT_DB_FILE):
    global _engine
    if _engine is None:
        init_database(db_file)

    return orm.Session(_engine)
