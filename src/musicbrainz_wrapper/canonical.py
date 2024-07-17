import csv
import datetime
import io
import logging
import pathlib
import re
import tarfile


from typing import List

from dateutil import parser
import requests
import sqlalchemy as sa
import zstandard
from requests.adapters import HTTPAdapter
from sqlalchemy import orm, PrimaryKeyConstraint
from urllib3 import Retry
import urllib3.util

from .datatypes import ReleaseID, ReleaseGroupID, RecordingID, ArtistID

from io import TextIOWrapper

_logger = logging.getLogger(__name__)

_engine = None

_DEFAULT_DB_FILE: pathlib.Path = pathlib.Path("mb_canonical.db")

_CANONICAL_DATA_URL: str = "https://data.metabrainz.org/pub/musicbrainz/canonical_data/"


class Base(orm.DeclarativeBase):
    pass

class Configuration(Base):
    __tablename__ = 'configuration'

    attribute: orm.Mapped[str] = orm.mapped_column(primary_key=True, sqlite_on_conflict_primary_key='REPLACE')
    value: orm.Mapped[str] = orm.mapped_column()


class CanonicalReleaseMapping(Base):
    __tablename__ = 'canonical_release_mapping'

    release_mbid: orm.Mapped[ReleaseID] = orm.mapped_column(sa.types.Uuid, primary_key=True, sqlite_on_conflict_primary_key='REPLACE')
    canonical_release_mbid: orm.Mapped[ReleaseID] = orm.mapped_column(sa.types.Uuid)
    release_group_mbid: orm.Mapped[ReleaseGroupID] = orm.mapped_column(sa.types.Uuid)


class CanonicalRecordingMapping(Base):
    __tablename__ = 'canonical_recording_mapping'

    recording_mbid: orm.Mapped[RecordingID] = orm.mapped_column(sa.types.Uuid, primary_key=True, sqlite_on_conflict_primary_key='REPLACE')
    canonical_recording_mbid: orm.Mapped[RecordingID] = orm.mapped_column(sa.types.Uuid)
    canonical_release_mbid: orm.Mapped[ReleaseID] = orm.mapped_column(sa.types.Uuid)


class ArtistCredit(Base):
    __tablename__ = 'artist_credit'

    artist_credit_id: orm.Mapped[int] = orm.mapped_column(primary_key=True, sqlite_on_conflict_primary_key='REPLACE')
    artist_credit_name: orm.Mapped[str] = orm.mapped_column()

    artist_mbids: orm.Mapped[List[ArtistID]] = orm.relationship(
        "ArtistCreditArtist",
        cascade="all", lazy="selectin"
    )


class ArtistCreditArtist(Base):
    __tablename__ = 'artist_credit_artist'
    artist_credit_id: orm.Mapped[int] = orm.mapped_column(sa.ForeignKey("artist_credit.artist_credit_id"),
                                                          primary_key=True, sqlite_on_conflict_primary_key='REPLACE')
    artist_mbid: orm.Mapped[ArtistID] = orm.mapped_column(sa.types.Uuid)


class CanonicalMetadata(Base):
    __tablename__ = 'canonical_metadata'

    id: orm.Mapped[int] = orm.mapped_column( primary_key=True, sqlite_on_conflict_primary_key='REPLACE')
    artist_credit_id: orm.Mapped[int] = orm.mapped_column(sa.ForeignKey("artist_credit.artist_credit_id"))
    release_mbid: orm.Mapped[ReleaseID] = orm.mapped_column(sa.types.Uuid)
    release_name: orm.Mapped[str] = orm.mapped_column(index=True)
    recording_mbid: orm.Mapped[RecordingID] = orm.mapped_column(sa.types.Uuid)
    recording_name: orm.Mapped[str] = orm.mapped_column(index=True)
    combined_lookup: orm.Mapped[str] = orm.mapped_column(index=True)
    score: orm.Mapped[int] = orm.mapped_column(index=True)



def init_database(
        db_file: pathlib.Path = None, db_url: str = None, echo_sql: bool = False
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

def get_canonical_dump_url(req_session: requests.Session = None) -> urllib3.util.Url:
    _logger.debug("Determining latest Musicbrainz canonical dump")

    base_url = urllib3.util.parse_url("https://data.metabrainz.org/pub/musicbrainz/canonical_data/")

    if req_session is None:
        req_session = requests.Session()

    res = req_session.get(base_url)
    match = max(re.findall(r"href=\"musicbrainz-canonical-dump-(.*?)/\"", res.text))
    url = urllib3.util.parse_url(base_url.url + f"musicbrainz-canonical-dump-{match}/musicbrainz-canonical-dump-{match}.tar.zst")
    return url

def get_canonical_dump(url: urllib3.util.Url = None, req_session: requests.Session = None, db_session=None, batch_size: int = 100000, force: bool = False):


    if req_session is None:
        req_session = requests.Session()
        retries = Retry(
            total=3,
            backoff_factor=0.1,
            status_forcelist=[502, 503, 504],
            allowed_methods={'POST'},
        )
        req_session.mount('https://', HTTPAdapter(max_retries=retries))

    if url is None:
        url = get_canonical_dump_url(req_session)

    _logger.info(f"Retrieving canonical musicbrainz data from {url}")

    if db_session is None:
        db_session = get_session()

    import_complete = db_session.scalar(
        sa.select(Configuration).where(Configuration.attribute == "import_complete"))
    if import_complete is not None and import_complete.value != 1:
        _logger.warning("Incomplete import found")
        force=True

    with req_session.get(url, stream=True) as req:
        req_buf = io.BufferedReader(req.raw)
        with zstandard.open(req_buf, mode='rb') as zstd_file:
            with tarfile.open(fileobj=zstd_file, mode='r:') as tar_file:
                while (member := tar_file.next()) is not None:
                    if not member.isfile():
                        continue

                    _logger.debug(f"Processing {member.name}")
                    fo = tar_file.extractfile(member)
                    filename = member.name.rsplit('/')[-1]
                    match filename:
                        case "TIMESTAMP":

                            latest_import_config = db_session.scalar(
                                sa.select(Configuration).where(Configuration.attribute == "latest_import"))

                            latest_import = latest_import_config.value if latest_import_config is not None else None
                            latest_import_asdate = parser.parse(latest_import) if latest_import is not None else None

                            timestamp = fo.read().decode()
                            timestamp_asdate = parser.parse(timestamp)

                            if force or latest_import is None or timestamp > latest_import:
                                # check versus known timestamp, delete all if needed
                                _logger.info("Newer dataset found, removing old one")
                                db_session.execute(
                                    sa.insert(Configuration).
                                    values({"attribute": "latest_import", "value": timestamp}))
                                db_session.execute(
                                    sa.insert(Configuration).values({"attribute": "import_complete", "value": 0}))
                                db_session.commit()

                            else:
                                _logger.info("Already working with latest dataset.")
                                return

                        case "COPYING":
                            # _logger.debug(fo.read().decode())
                            pass
                        case "canonical_musicbrainz_data.csv":
                            _logger.info("Importing Canonical Musicbrainz Metadata")
                            with TextIOWrapper(fo, encoding='utf-8') as tw:
                                csvreader = csv.DictReader(tw)
                                row: dict
                                i = 1
                                while next_rows := [next(csvreader, None) for i in range(0, batch_size)]:
                                    if all(x is None for x in next_rows):
                                        break

                                    _logger.debug(f"Importing rows {i} - {i + batch_size - 1}")
                                    i = i + batch_size

                                    acas = [{
                                        "artist_credit_id": int(row['artist_credit_id']),
                                        "artist_mbid": ArtistID(artist_mbid)
                                    } for row in next_rows if row is not None for artist_mbid in
                                        row['artist_mbids'].split(',')]
                                    stmt = sa.insert(ArtistCreditArtist)
                                    db_session.execute(stmt, acas)

                                    acs = [{
                                        "artist_credit_id": int(row['artist_credit_id']),
                                        "artist_credit_name": row['artist_credit_name']
                                    } for row in next_rows if row is not None]
                                    stmt = sa.insert(ArtistCredit)
                                    db_session.execute(stmt, acs)

                                    cmds = [{
                                        "artist_credit_id": int(row['artist_credit_id']),
                                        "release_mbid": ReleaseID(row['release_mbid']),
                                        "release_name": row['release_name'],
                                        "recording_mbid": RecordingID(row['recording_mbid']),
                                        "recording_name": row['recording_name'],
                                        "combined_lookup": row['combined_lookup'],
                                        "score": int(row['score'])
                                    } for row in next_rows if row is not None]
                                    stmt = sa.insert(CanonicalMetadata)

                                    db_session.execute(stmt, cmds)

                                    db_session.commit()
                            _logger.debug("Done importing")

                        case "canonical_recording_redirect.csv":
                            _logger.info("Importing Canonical Musicbrainz Recording Redirects")
                            with TextIOWrapper(fo, encoding='utf-8') as tw:
                                csvreader = csv.DictReader(tw)
                                row: dict
                                i = 1
                                while next_rows := [next(csvreader, None) for i in range(0, batch_size)]:
                                    if all(x is None for x in next_rows):
                                        break

                                    _logger.debug(f"Importing rows {i} - {i + batch_size - 1}")
                                    i = i + batch_size

                                    crms = [{
                                        "canonical_recording_mbid": RecordingID(row['canonical_recording_mbid']),
                                        "canonical_release_mbid": ReleaseID(row['canonical_release_mbid']),
                                        "recording_mbid": RecordingID(row['recording_mbid'])
                                    } for row in next_rows if row is not None]
                                    stmt = sa.insert(CanonicalRecordingMapping)
                                    db_session.execute(stmt, crms)

                                    db_session.commit()
                            _logger.debug("Done importing")

                        case "canonical_release_redirect.csv":
                            _logger.info("Importing Canonical Musicbrainz Release Redirects")
                            with TextIOWrapper(fo, encoding='utf-8') as tw:
                                csvreader = csv.DictReader(tw)
                                row: dict
                                i = 1
                                while next_rows := [next(csvreader, None) for i in range(0, batch_size)]:
                                    if all(x is None for x in next_rows):
                                        break

                                    _logger.debug(f"Importing rows {i} - {i + batch_size - 1}")
                                    i = i + batch_size

                                    crms = [{
                                        "canonical_release_mbid": ReleaseID(row['canonical_release_mbid']),
                                        "release_group_mbid": ReleaseID(row['release_group_mbid']),
                                        "release_mbid": ReleaseID(row['release_mbid'])
                                    } for row in next_rows if row is not None]
                                    stmt = sa.insert(CanonicalReleaseMapping)
                                    db_session.execute(stmt, crms)

                                    db_session.commit()
                            _logger.debug("Done importing")

                        case _:
                            print(f"Don't know how to handle {filename}")
                            break
    db_session.execute(
        sa.insert(Configuration).values({"attribute": "import_complete", "value": 1}))
    db_session.commit()