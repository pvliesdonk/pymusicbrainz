import csv
import io
import logging
import os
import pathlib
import re
import tarfile
import tempfile
from io import TextIOWrapper
from typing import List

import requests
import sqlalchemy as sa
import urllib3.util
import zstandard
from dateutil import parser
from requests.adapters import HTTPAdapter
from requests_file import FileAdapter
from sqlalchemy import orm
from urllib3 import Retry

from .api import get_datadir
from .datatypes import ReleaseID, ReleaseGroupID, RecordingID, ArtistID

_logger = logging.getLogger(__name__)

_engine = None

_DEFAULT_DB_FILE: pathlib.Path = pathlib.Path("mb_canonical.db")

_CANONICAL_DATA_URL: str = "https://data.metabrainz.org/pub/musicbrainz/canonical_data/"


class Base(orm.DeclarativeBase):
    pass

class Configuration(Base):
    __tablename__ = 'configuration'

    attribute: orm.Mapped[str] = orm.mapped_column(primary_key=True, sqlite_on_conflict_primary_key='IGNORE')
    value: orm.Mapped[str] = orm.mapped_column()


class CanonicalReleaseMapping(Base):
    __tablename__ = 'canonical_release_mapping'

    release_mbid: orm.Mapped[ReleaseID] = orm.mapped_column(sa.types.Uuid, primary_key=True, sqlite_on_conflict_primary_key='IGNORE')
    canonical_release_mbid: orm.Mapped[ReleaseID] = orm.mapped_column(sa.types.Uuid)
    release_group_mbid: orm.Mapped[ReleaseGroupID] = orm.mapped_column(sa.types.Uuid)


class CanonicalRecordingMapping(Base):
    __tablename__ = 'canonical_recording_mapping'

    recording_mbid: orm.Mapped[RecordingID] = orm.mapped_column(sa.types.Uuid, primary_key=True, sqlite_on_conflict_primary_key='IGNORE')
    canonical_recording_mbid: orm.Mapped[RecordingID] = orm.mapped_column(sa.types.Uuid)
    canonical_release_mbid: orm.Mapped[ReleaseID] = orm.mapped_column(sa.types.Uuid)


class ArtistCredit(Base):
    __tablename__ = 'artist_credit'

    artist_credit_id: orm.Mapped[int] = orm.mapped_column(primary_key=True, sqlite_on_conflict_primary_key='IGNORE')
    artist_credit_name: orm.Mapped[str] = orm.mapped_column()

    artist_mbids: orm.Mapped[List[ArtistID]] = orm.relationship(
        "ArtistCreditArtist",
        cascade="all", lazy="selectin"
    )


class ArtistCreditArtist(Base):
    __tablename__ = 'artist_credit_artist'
    artist_credit_id: orm.Mapped[int] = orm.mapped_column(sa.ForeignKey("artist_credit.artist_credit_id"),
                                                          primary_key=True, sqlite_on_conflict_primary_key='IGNORE')
    artist_mbid: orm.Mapped[ArtistID] = orm.mapped_column(sa.types.Uuid)


class CanonicalMetadata(Base):
    __tablename__ = 'canonical_metadata'

    id: orm.Mapped[int] = orm.mapped_column( primary_key=True, sqlite_on_conflict_primary_key='IGNORE')
    artist_credit_id: orm.Mapped[int] = orm.mapped_column(sa.ForeignKey("artist_credit.artist_credit_id"))
    release_mbid: orm.Mapped[ReleaseID] = orm.mapped_column(sa.types.Uuid, index=True)
    release_name: orm.Mapped[str] = orm.mapped_column()
    recording_mbid: orm.Mapped[RecordingID] = orm.mapped_column(sa.types.Uuid, index=True)
    recording_name: orm.Mapped[str] = orm.mapped_column()
    combined_lookup: orm.Mapped[str] = orm.mapped_column(index=True)
    score: orm.Mapped[int] = orm.mapped_column(index=True)

# select cm.*, ac.artist_credit_name, aca.artist_mbid
# from canonical_metadata cm
# join main.artist_credit ac on ac.artist_credit_id = cm.artist_credit_id
# join main.artist_credit_artist aca on cm.artist_credit_id = aca.artist_credit_id;






def init_database(echo_sql: bool = False):
    global _engine
    # Create a database connection

    db_file = get_datadir() / 'mb_canonical.db'
    _logger.debug(f"Using sqlite3 database in file {db_file.absolute()}")
    database_url = f"sqlite+pysqlite:///{db_file.as_posix()}"

    _logger.debug(f"Opening/creating database as {database_url}")
    _engine = sa.create_engine(database_url, echo=echo_sql)

    #view_stmt = sa.select(CanonicalMetadata).join(ArtistCredit).join(ArtistCreditArtist)
    #create_view('canonical_release_mapping_all', view_stmt, Base.metadata)

    Base.metadata.create_all(_engine)


def get_session(db_file: pathlib.Path = _DEFAULT_DB_FILE):
    global _engine
    if _engine is None:
        init_database(False)

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

def get_canonical_dump(url: urllib3.util.Url|str = None, req_session: requests.Session = None, db_session=None, batch_size: int = 1000000, force: bool = False):


    if req_session is None:
        req_session = requests.Session()
        retries = Retry(
            total=3,
            backoff_factor=0.1,
            status_forcelist=[502, 503, 504],
            allowed_methods={'POST'},
        )
        req_session.mount('https://', HTTPAdapter(max_retries=retries))
        req_session.mount('file://', FileAdapter())

    if url is None:
        url = get_canonical_dump_url(req_session)
    if isinstance(url, str):
        url = urllib3.util.parse_url(url)

    _logger.info(f"Retrieving canonical musicbrainz data from {url}")

    if db_session is None:
        db_session = get_session()



    latest_import_config = db_session.scalar(
        sa.select(Configuration).where(Configuration.attribute == "latest_import"))

    latest_import = latest_import_config.value if latest_import_config is not None else None
    latest_import_asdate = parser.parse(latest_import) if latest_import is not None else None
    if latest_import_asdate is not None:
        _logger.info(f"Latest import is from {latest_import_asdate.date().isoformat()}")
    else:
        _logger.info(f"Don't know how old the database is")

    import_complete = db_session.scalar(
        sa.select(Configuration).where(Configuration.attribute == "import_complete"))
    if import_complete is None or (import_complete is not None and import_complete.value != '1'):
        _logger.warning("Incomplete import found")
        force = True
    else:
        _logger.info("Database integrity intact")


    if not force:
        # try to determine version from filename:
        match = re.search(r"dump-(\d\d\d\d\d\d\d\d)-\d\d\d\d\d\d", url.url)
        if match is not None:

            url_date = match.group(1)
            url_date_asdate = parser.parse(url_date)
            _logger.info(f"Found new database for date {url_date_asdate.date().isoformat()}")

            if (not latest_import_asdate is None) and (not url_date_asdate > latest_import_asdate):
                    _logger.info("Latest version of canonical data seems to be in the database already")
                    return
            else:
                _logger.info("Found newer database. Downloading.")

        else:
            _logger.debug(f"Could not determine date from url {url}")


    if url.scheme == "file":
        file = pathlib.Path(url.path).resolve(strict=True)
        _logger.info(f"reading data file from {file}")
        zstd_file = zstandard.open(file, mode='rb' )
    else:
        fd = tempfile.TemporaryFile()
        with req_session.get(url, stream=True) as r:
            for chunk in r.iter_content(chunk_size=1024):
                fd.write(chunk)

        _logger.info("Downloaded raw data files")
        fd.seek(0)
        zstd_file = zstandard.open(fd, mode='rb')

    tar_file = tarfile.open(fileobj=zstd_file, mode='r:')

    while (member := tar_file.next()) is not None:
        if not member.isfile():
            continue

        _logger.debug(f"Processing {member.name}")
        fo = tar_file.extractfile(member)
        filename = member.name.rsplit('/')[-1]
        match filename:
            case "TIMESTAMP":



                timestamp = fo.read().decode()
                timestamp_asdate = parser.parse(timestamp)

                if force or latest_import is None or timestamp > latest_import:
                    # check versus known timestamp, delete all if needed
                    _logger.info("Newer dataset found, removing old one")
                    db_session.execute(sa.delete(CanonicalReleaseMapping))
                    db_session.execute(sa.delete(CanonicalRecordingMapping))
                    db_session.execute(sa.delete(ArtistCredit))
                    db_session.execute(sa.delete(ArtistCreditArtist))
                    db_session.execute(sa.delete(CanonicalMetadata))
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
