import datetime
import enum
import logging
import pathlib
import re
import uuid
from abc import ABC
from functools import cached_property
from typing import Sequence, Mapping

import acoustid
import mbdata.models
import musicbrainzngs
import rapidfuzz
import sqlalchemy as sa
import typesense
from sqlalchemy import orm
from unidecode import unidecode
import urllib3.util

_logger = logging.getLogger(__name__)

_DEFAULT_APP: str = "My Tagger"
_DEFAULT_VERSION: str = "0.1"
_DEFAULT_CONTACT: str = "https://music.liesdonk.nl"
_DEFAULT_API_URL: str = "musicbrainz.org"
_DEFAULT_HTTPS: bool = True
_DEFAULT_RATE_LIMIT: bool = True
_DEFAULT_DB_URI: str = 'postgresql://musicbrainz:musicbrainz@127.0.0.1/musicbrainz'

ACOUSTID_APIKEY = "7z40OrGgVS"


def split_artist(artist_query: str) -> list[str]:
    result = [artist_query]

    splits = [" & ", " + ", " ft. ", " vs. ", " Ft. ", " feat. ", " and ", " en "]

    for split in splits:
        result = re.split(split, artist_query, flags=re.IGNORECASE)
        if len(result) > 1:
            for r in result:
                if r not in result:
                    result.append(r)

    return result


def _fold_sort_candidates(
        candidates: Sequence[tuple["ReleaseGroup", "Recording"]])\
        -> list[tuple["ReleaseGroup", list["Recording"]]]:
    t1 = {}
    for (rg, recording) in candidates:
        if rg in t1.keys():
            t1[rg].append(recording)
        else:
            t1[rg] = [recording]

    t2 = sorted([(k, sorted(v)) for k, v in t1.items()], key=lambda x: x[0])
    return t2


def flatten_title(artist_name="", recording_name="", album_name="") -> str:
    """ Given the artist name and recording name, return a combined_lookup string """
    return unidecode(re.sub(r'\W+', '', artist_name + album_name + recording_name).lower())


def parse_partial_date(pdate: mbdata.models.PartialDate) -> datetime.date | None:
    if pdate.year is None:
        return None
    if pdate.month is None:
        return datetime.date(year=pdate.year, month=1, day=1)
    if pdate.day is None:
        return datetime.date(year=pdate.year, month=pdate.month, day=1)
    return datetime.date(year=pdate.year, month=pdate.month, day=pdate.day)


class MBApiError(Exception):
    pass


class IncomparableError(MBApiError):
    pass


class NotFoundError(MBApiError):
    pass


class NotConfiguredError(MBApiError):
    pass


class MBID(uuid.UUID):
    """Abstract representation of a Musicbrainz Identifier"""
    pass


class ArtistID(MBID):
    """Musicbrainz Artist ID"""
    pass


class ReleaseGroupID(MBID):
    """Musicbrainz Release Group ID"""
    pass


class ReleaseID(MBID):
    """Musicbrainz Release ID"""
    pass


class RecordingID(MBID):
    """Musicbrainz Recording ID"""
    pass


class MediumID(MBID):
    """Musicbrainz Medium ID"""
    pass


class TrackID(MBID):
    """Musicbrainz Track ID"""
    pass


class WorkID(MBID):
    """Musicbrainz Work ID"""
    pass


_engine = None
_Session = None


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

    mbdata.models.Base.metadata.create_all(_engine)


def get_db_session():
    global _engine
    if _engine is None or _Session is None:
        init_database(_DEFAULT_DB_URI)

    return _Session()


class MusicBrainzObject(ABC):
    """Abstract object representing any of the primary Musicbrainz entities"""
    pass


class ReleaseType(enum.StrEnum):
    """Constants for the different Musicbrainz release types"""
    NAT = "Nat"
    ALBUM = "Album"
    SINGLE = "Single"
    EP = "EP"
    BROADCAST = "Broadcast"
    OTHER = "Other"
    COMPILATION = "Compilation"
    SOUNDTRACK = "Soundtrack"
    SPOKENWORD = "Spokenword"
    INTERVIEW = "Interview"
    AUDIOBOOK = "Audiobook"
    LIVE = "Live"
    REMIX = "Remix"
    DJ_MIX = "DJ-mix"
    MIXTAPE = "Mixtape/Street"
    DEMO = "Demo"
    AUDIODRAMA = "Audio drama"
    FIELDRECORDING = "Field recording"


PRIMARY_TYPES = {
    ReleaseType.ALBUM: 1,
    ReleaseType.SINGLE: 2,
    ReleaseType.EP: 3,
    ReleaseType.OTHER: 11,
    ReleaseType.BROADCAST: 12}
"""Mapping of primary release types to identifiers used in Musicbrainz Database schema"""

SECONDARY_TYPES = {
    ReleaseType.COMPILATION: 1,
    ReleaseType.SOUNDTRACK: 2,
    ReleaseType.SPOKENWORD: 3,
    ReleaseType.INTERVIEW: 4,
    ReleaseType.AUDIOBOOK: 5,
    ReleaseType.LIVE: 6,
    ReleaseType.REMIX: 7,
    ReleaseType.DJ_MIX: 8,
    ReleaseType.MIXTAPE: 9,
    ReleaseType.DEMO: 10,
    ReleaseType.AUDIODRAMA: 11,
    ReleaseType.FIELDRECORDING: 12
}
"""Mapping of secondary release types to identifiers used in Musicbrainz Database schema"""


class ReleaseStatus(enum.StrEnum):
    """Constants for the various Musicbrainz release statuses"""
    OFFICIAL = "Official"
    PROMOTION = "Promotion"
    BOOTLEG = "Bootleg"
    PSEUDO = "Pseudo-Release"


UNKNOWN_ARTIST_ID = ArtistID("125ec42a-7229-4250-afc5-e057484327fe")
"""Artist ID representing an Unknown Artist"""

VA_ARTIST_ID = ArtistID("89ad4ac3-39f7-470e-963a-56509c546377")
"""Artist ID representing Various Artists"""


class Artist(MusicBrainzObject):
    """Class representing an artist"""

    def __init__(self,
                 in_obj: ArtistID | mbdata.models.Artist | str) -> None:
        with get_db_session() as session:
            if isinstance(in_obj, mbdata.models.Artist):
                a = session.merge(in_obj)
            else:
                if isinstance(in_obj, str):
                    in_obj = ArtistID(in_obj)
                stmt = sa.select(mbdata.models.Artist).where(mbdata.models.Artist.gid == str(in_obj))
                a: mbdata.models.Artist = session.scalar(stmt)

            self.id: ArtistID = ArtistID(str(a.gid))
            self._db_id: int = a.id
            self.name: str = a.name
            self.artist_type: str = a.type.name if a.type is not None else None
            self.sort_name: str = a.sort_name
            self.disambiguation: str = a.comment

    @cached_property
    def aliases(self) -> list[str]:
        with get_db_session() as session:
            stmt = sa.select(mbdata.models.ArtistAlias).where(mbdata.models.ArtistAlias.artist.has(id=self._db_id))
            result = session.scalars(stmt)
            out = [alias.name for alias in result]
            return out

    @cached_property
    def url(self) -> str:
        return f"https://musicbrainz.org/artist/{self.id}"

    def _release_group_query(self,
                             primary_type: ReleaseType = None,
                             secondary_types: list[ReleaseType] | None = [],
                             credited: bool = True,
                             contributing: bool = False) -> sa.Select:
        # base: all release release groups for artist
        stmt = sa.select(mbdata.models.ReleaseGroup). \
            distinct(). \
            join(mbdata.models.ArtistReleaseGroup). \
            where(mbdata.models.ArtistReleaseGroup.artist.has(id=self._db_id)). \
            where(~mbdata.models.ArtistReleaseGroup.unofficial)

        # credited/contributing
        if credited:
            if not contributing:
                stmt = stmt.where(~mbdata.models.ArtistReleaseGroup.is_track_artist)
        else:
            if contributing:
                stmt = stmt.where(mbdata.models.ArtistReleaseGroup.is_track_artist)
            else:
                raise MBApiError("Query would result in no release groups")

        # primary type
        if primary_type is not None:
            stmt = stmt.where(mbdata.models.ArtistReleaseGroup.primary_type == PRIMARY_TYPES[primary_type])

        if secondary_types is not None:
            if len(secondary_types) > 0:
                types = [SECONDARY_TYPES[t] for t in secondary_types]
                where_clause = mbdata.models.ArtistReleaseGroup.secondary_types.contains(types)
                stmt = stmt.where(where_clause)
        else:
            stmt = stmt.where(mbdata.models.ArtistReleaseGroup.secondary_types.is_(None))

        return stmt

    def get_release_groups(self,
                           primary_type: ReleaseType = None,
                           secondary_types: list[ReleaseType] | None = [],
                           credited: bool = True,
                           contributing: bool = False) -> list["ReleaseGroup"]:

        s = f"Fetching"
        if primary_type is not None:
            s = s + f" {primary_type}s"
        else:
            s = s + " release groups"
        s = s + f" {'credited to' if credited else ''}{'/' if credited and contributing else ''}{'contributed to by' if contributing else ''}"
        s = s + f" artist {self.name} [{self.id}]"
        if secondary_types is not None:
            if len(secondary_types) > 0:
                s = s + f" with secondary types {', '.join(secondary_types)}"
        else:
            s = s + f" with no secondary types"
        _logger.debug(s)

        with get_db_session() as session:
            stmt = self._release_group_query(primary_type=primary_type, secondary_types=secondary_types,
                                             credited=credited, contributing=contributing)
            result: list[mbdata.models.ReleaseGroup] = session.scalars(stmt).all()

        return [get_release_group(rg) for rg in result]

    @cached_property
    def release_groups(self) -> list["ReleaseGroup"]:
        return self.get_release_groups(primary_type=None, secondary_types=[], credited=True, contributing=False)

    @cached_property
    def albums(self) -> list["ReleaseGroup"]:
        return self.get_release_groups(primary_type=ReleaseType.ALBUM, secondary_types=[], credited=True,
                                       contributing=False)

    @cached_property
    def singles(self) -> list["ReleaseGroup"]:
        return self.get_release_groups(primary_type=ReleaseType.SINGLE, secondary_types=[], credited=True,
                                       contributing=False)

    @cached_property
    def eps(self) -> list["ReleaseGroup"]:
        return self.get_release_groups(primary_type=ReleaseType.EP, secondary_types=[], credited=True,
                                       contributing=False)

    @cached_property
    def studio_albums(self) -> list["ReleaseGroup"]:
        return self.get_release_groups(primary_type=ReleaseType.ALBUM, secondary_types=None, credited=True,
                                       contributing=False)

    @cached_property
    def soundtracks(self) -> list["ReleaseGroup"]:
        return self.get_release_groups(primary_type=ReleaseType.ALBUM, secondary_types=[ReleaseType.SOUNDTRACK],
                                       credited=True, contributing=True)

    def is_sane(self, artist_query: str, cut_off=70) -> bool:

        artist_split = split_artist(artist_query)

        artist_ratios = [rapidfuzz.process.extractOne(
            flatten_title(artist_name=split),
            [flatten_title(self.name)] + [flatten_title(a) for a in self.aliases],
            processor=rapidfuzz.utils.default_process
        )[1] for split in artist_split]
        artist_ratio = max(artist_ratios)
        if artist_ratio < cut_off:
            _logger.debug(f"{self} is not a sane candidate for artist {artist_query}")
        return artist_ratio > cut_off

    def __repr__(self):
        if self.disambiguation is not None:
            return f"Artist:  {self.name} [{self.id}] ({self.disambiguation})"
        else:
            return f"Artist:  {self.name} [{self.id}]"

    def __eq__(self, other):
        if isinstance(other, Artist):
            return self.id == other.id
        else:
            return False

    def __lt__(self, other):
        return self.sort_name < other.sort_name

    def __contains__(self, item):
        if isinstance(item, Release):
            raise NotImplementedError
            return any([release_id == item.id for release_id in self.release_ids])
        if isinstance(item, ReleaseID):
            raise NotImplementedError
            return any([release_id == item for release_id in self.release_ids])
        if isinstance(item, ReleaseGroup):
            return any([rg == item for rg in self.release_groups])
        if isinstance(item, ReleaseGroupID):
            return any([rg.id == item for rg in self.release_groups])
        if isinstance(item, Recording):
            raise NotImplementedError
            return any([recording_id == item.id for recording_id in self.recording_ids])
        if isinstance(item, RecordingID):
            raise NotImplementedError
            return any([recording_id == item for recording_id in self.recording_ids])

    def __hash__(self):
        return hash(self.id)


class ReleaseGroup(MusicBrainzObject):

    def __init__(self,
                 in_obj: ReleaseGroupID | mbdata.models.ReleaseGroup | str) -> None:
        with get_db_session() as session:
            if isinstance(in_obj, mbdata.models.ReleaseGroup):

                rg: mbdata.models.ReleaseGroup = session.merge(in_obj)
            else:
                if isinstance(in_obj, str):
                    in_obj = ReleaseGroupID(in_obj)
                stmt = sa.select(mbdata.models.ReleaseGroup).where(mbdata.models.ReleaseGroup.gid == str(in_obj))
                rg: mbdata.models.ReleaseGroup = session.scalar(stmt)

            self.id: ReleaseGroupID = ReleaseGroupID(str(rg.gid))
            self._db_id: int = rg.id
            self.artists = [get_artist(ArtistID(str(a.artist.gid))) for a in rg.artist_credit.artists]
            self.title: str = rg.name
            self.primary_type: ReleaseType = ReleaseType(rg.type.name) if rg.type is not None else None
            self.types: list[ReleaseType] = ([self.primary_type] if self.primary_type is not None else []) + [
                ReleaseType(s.secondary_type.name) for s in rg.secondary_types]
            self.disambiguation: str = rg.comment
            self.artist_credit_phrase: str = rg.artist_credit.name
            self.is_va: bool = (rg.artist_credit_id == 1)

    @cached_property
    def url(self) -> str:
        return f"https://musicbrainz.org/release-group/{self.id}"

    @cached_property
    def is_studio_album(self) -> bool:
        return self.primary_type == ReleaseType.ALBUM and len(self.types) == 1

    @cached_property
    def is_single(self) -> bool:
        return self.primary_type == ReleaseType.SINGLE

    @cached_property
    def is_soundtrack(self) -> bool:
        return self.primary_type == ReleaseType.ALBUM and ReleaseType.SOUNDTRACK in self.types

    @cached_property
    def is_compilation(self) -> bool:
        return ReleaseType.COMPILATION in self.types

    @cached_property
    def is_eps(self) -> bool:
        return self.primary_type == ReleaseType.EP

    @cached_property
    def first_release_date(self) -> datetime.date | None:
        with get_db_session() as session:
            stmt = sa.select(mbdata.models.ReleaseGroupMeta).where(mbdata.models.ReleaseGroupMeta.id == self._db_id)
            rgm: mbdata.models.ReleaseGroupMeta = session.scalar(stmt)

            return parse_partial_date(rgm.first_release_date)

    @cached_property
    def aliases(self) -> list[str]:
        result = [self.title]
        with get_db_session() as session:
            stmt = sa.select(mbdata.models.ReleaseGroupAlias).where(
                mbdata.models.ReleaseGroupAlias.release_group_id == self._db_id)
            rgas: list[mbdata.models.ReleaseGroupAlias] = session.scalars(stmt).all()

            for rga in rgas:
                if rga.name not in result:
                    result.append(rga.name)
        return result

    @cached_property
    def releases(self) -> list["Release"]:
        with get_db_session() as session:
            stmt = sa.select(mbdata.models.Release).where(mbdata.models.Release.release_group_id == self._db_id)
            releases: list[mbdata.models.Release] = session.scalars(stmt).all()

            return [get_release(ReleaseID(str(release.gid))) for release in releases]

    @cached_property
    def recordings(self) -> list["Recording"]:
        result = []
        rel: Release
        for rel in self.releases:
            for rec in rel.recordings:
                if rec not in result:
                    result.append(rec)
        return result

    def is_sane(self, artist_query: str, title_query: str, cut_off=70) -> bool:
        artist_ratio = rapidfuzz.fuzz.WRatio(
            flatten_title(artist_name=self.artist_credit_phrase),
            flatten_title(artist_name=artist_query),
            processor=rapidfuzz.utils.default_process,
            score_cutoff=cut_off
        )
        if artist_ratio < cut_off:
            _logger.warning(f"{self} is not a sane candidate for artist {artist_query}")
        title_ratio = rapidfuzz.process.extractOne(
            flatten_title(album_name=title_query),
            [flatten_title(album_name=self.title)] + [flatten_title(album_name=x) for x in self.aliases],
            processor=rapidfuzz.utils.default_process
        )[1]
        if title_ratio < cut_off:
            _logger.warning(f"{self} is not a sane candidate for title {title_query}")
        return artist_ratio > cut_off and title_ratio > cut_off

    def __repr__(self):
        s1 = f" [{self.primary_type}]" if self.primary_type is not None else ""
        s2 = (
            f" {self.first_release_date}" if self.first_release_date is not None else ""
        )
        return f"Release Group:  {self.artist_credit_phrase} - {self.title}{s1}{s2} [{self.id}]"

    def __eq__(self, other):
        if isinstance(other, ReleaseGroup):
            return self.id == other.id
        else:
            return False

    def __lt__(self, other):
        if isinstance(other, ReleaseGroup):

            if self.first_release_date is not None:
                if other.first_release_date is not None:
                    return self.first_release_date < other.first_release_date
                else:
                    return True
            else:
                return False

    def __contains__(self, item):
        if isinstance(item, Artist):
            return any([artist_id == item.id for artist_id in self.artist_ids])
        if isinstance(item, ArtistID):
            return any([artist_id == item for artist_id in self.artist_ids])
        if isinstance(item, Release):
            return any([release_id == item.id for release_id in self.release_ids])
        if isinstance(item, ReleaseID):
            return any([release_id == item for release_id in self.release_ids])
        if isinstance(item, Recording):
            return any([recording_id == item.id for recording_id in self.recording_ids])
        if isinstance(item, RecordingID):
            return any([recording_id == item for recording_id in self.recording_ids])

    def __hash__(self):
        return hash(self.id)


class Release(MusicBrainzObject):

    def __init__(self,
                 in_obj: ReleaseID | mbdata.models.Release | str) -> None:
        with get_db_session() as session:
            if isinstance(in_obj, mbdata.models.Release):
                rel: mbdata.models.Release = session.merge(in_obj)
            else:
                if isinstance(in_obj, str):
                    in_obj = ReleaseID(in_obj)
                stmt = sa.select(mbdata.models.Release).where(mbdata.models.Release.gid == str(in_obj))
                rel: mbdata.models.Release = session.scalar(stmt)

            self.id: ReleaseID = ReleaseID(str(rel.gid))
            self._db_id: int = rel.id
            self.artists = [get_artist(ArtistID(str(a.artist.gid))) for a in rel.artist_credit.artists]
            self.title: str = rel.name
            self._release_group_id: ReleaseGroupID = ReleaseGroupID(str(rel.release_group.gid))
            self.artist_credit_phrase: str = rel.artist_credit.name
            self.disambiguation: str = rel.comment
            self.first_release_date: datetime.date = parse_partial_date(
                rel.first_release.date) if rel.first_release is not None else None

    @cached_property
    def aliases(self) -> list[str]:
        result = [self.title]
        with get_db_session() as session:
            stmt = sa.select(mbdata.models.ReleaseAlias).where(
                mbdata.models.ReleaseAlias.release_id == self._db_id)
            ras: list[mbdata.models.ReleaseAlias] = session.scalars(stmt).all()

            for ra in ras:
                if ra.name not in result:
                    result.append(ra.name)
        return result

    @cached_property
    def url(self) -> str:
        return f"https://musicbrainz.org/release/{self.id}"

    @cached_property
    def release_group(self) -> ReleaseGroup:
        return get_release_group(self._release_group_id)

    @cached_property
    def mediums(self) -> list["Medium"]:
        with get_db_session() as session:
            stmt = sa.select(mbdata.models.Medium).where(mbdata.models.Medium.release_id == str(self._db_id))
            ms: list[mbdata.models.Medium] = session.scalars(stmt).all()

            return [get_medium(m) for m in ms]

    @cached_property
    def tracks(self) -> list["Track"]:
        result = []
        for m in self.mediums:
            for t in m.tracks:
                if t not in result:
                    result.append(t)
        return result

    @cached_property
    def recordings(self) -> list["Recording"]:
        result = []
        for t in self.tracks:
            if t.recording not in result:
                result.append(t.recording)
        return result

    def is_sane(self, artist_query: str, title_query: str, cut_off=70) -> bool:
        artist_ratio = rapidfuzz.fuzz.WRatio(
            flatten_title(artist_name=self.artist_credit_phrase),
            flatten_title(artist_name=artist_query),
            processor=rapidfuzz.utils.default_process,
            score_cutoff=cut_off
        )
        if artist_ratio < cut_off:
            _logger.warning(f"{self} is not a sane candidate for artist {artist_query}")
        title_ratio = rapidfuzz.process.extractOne(
            flatten_title(recording_name=title_query),
            [flatten_title(recording_name=self.title)] + [flatten_title(recording_name=x) for x in self.aliases],
            processor=rapidfuzz.utils.default_process
        )[1]
        if title_ratio < cut_off:
            _logger.warning(f"{self} is not a sane candidate for title {title_query}")
        return artist_ratio > cut_off and title_ratio > cut_off

    def __repr__(self):
        return f"Release:  {self.artist_credit_phrase}: {self.title} [{self.id}]"

    def __eq__(self, other):
        if isinstance(other, Release):
            return self.id == other.id
        else:
            return False

    def __contains__(self, item):
        if isinstance(item, Artist):
            return any([artist_id == item.id for artist_id in self.artist_ids])
        if isinstance(item, ArtistID):
            return any([artist_id == item for artist_id in self.artist_ids])
        if isinstance(item, Recording):
            return any([recording_id == item.id for recording_id in self.recording_ids])
        if isinstance(item, RecordingID):
            return any([recording_id == item for recording_id in self.recording_ids])

    def __lt__(
            self,
            other):
        if isinstance(other, Release):
            if self.release_group != other.release_group:
                return self.release_group.first_release_date < other.release_group.first_release_date

            if self.date is not None and other.date is not None:
                return self.date < other.date
            else:
                return True
                #raise IncomparableError(self, other)
        return NotImplemented

    def __hash__(self):
        return hash(self.id)


class Recording(MusicBrainzObject):

    def __init__(self,
                 in_obj: RecordingID | mbdata.models.Recording | str) -> None:
        with get_db_session() as session:
            if isinstance(in_obj, mbdata.models.Recording):
                rec: mbdata.models.Recording = session.merge(in_obj)
            else:
                if isinstance(in_obj, str):
                    in_obj = RecordingID(in_obj)
                stmt = sa.select(mbdata.models.Recording).where(mbdata.models.Recording.gid == str(in_obj))
                rec: mbdata.models.Recording = session.scalar(stmt)

            self.id: RecordingID = RecordingID(str(rec.gid))
            self._db_id: int = rec.id
            self.artists = [get_artist(ArtistID(str(a.artist.gid))) for a in rec.artist_credit.artists]
            self.title: str = rec.name
            self.artist_credit_phrase: str = rec.artist_credit.name
            self.disambiguation: str = rec.comment
            self.first_release_date: datetime.date = parse_partial_date(
                rec.first_release.date) if rec.first_release is not None else None

    @cached_property
    def aliases(self) -> list[str]:
        result = [self.title]
        with get_db_session() as session:
            stmt = sa.select(mbdata.models.RecordingAlias).where(
                mbdata.models.RecordingAlias.recording_id == self._db_id)
            ras: list[mbdata.models.RecordingAlias] = session.scalars(stmt).all()

            for ra in ras:
                if ra.name not in result:
                    result.append(ra.name)
        return result

    @cached_property
    def performance_type(self) -> list[str]:
        p = self.performance_of
        return self.performance_type

    @cached_property
    def performance_of(self) -> "Work":
        with get_db_session() as session:
            stmt = sa.select(mbdata.models.LinkRecordingWork). \
                where(mbdata.models.LinkRecordingWork.entity0_id == str(self._db_id))
            res: mbdata.models.LinkRecordingWork = session.scalar(stmt)
            w = get_work(res.work)

            stmt = sa.select(mbdata.models.LinkAttribute). \
                where(mbdata.models.LinkAttribute.link == res.link)
            res2: list[mbdata.models.LinkAttribute] = session.scalars(stmt).all()
            types = []
            for att in res2:
                types.append(att.attribute_type.name)
            self.performance_type = types

        return w

    @cached_property
    def is_acapella(self) -> bool:
        return "acapella" in self.performance_type

    @cached_property
    def is_live(self) -> bool:
        return "live" in self.performance_type

    @cached_property
    def is_medley(self) -> bool:
        return "medley" in self.performance_type

    @cached_property
    def is_partial(self) -> bool:
        return "partial" in self.performance_type

    @cached_property
    def is_instrumental(self) -> bool:
        return "ïnstrumental" in self.performance_type

    @cached_property
    def is_cover(self) -> bool:
        return "cover" in self.performance_type

    @cached_property
    def is_karaoke(self) -> bool:
        return "karaoke" in self.performance_type

    @cached_property
    def is_normal_performance(self) -> bool:
        return len(self.performance_type) == 0

    @cached_property
    def siblings(self) -> list["Recording"]:
        result = []
        if len(self.performance_type) == 0:
            _logger.error("Appending regular performance siblings")
            work = self.performance_of
            for r in work.performances['no-attr']:
                if r not in result:
                    result.append(r)
            return result
        else:
            _logger.error(
                f"Recording is not a regular performance ({'/'.join(self.performance_type)}) for '{self.artist_credit_phrase}' - '{self.title}' [{self.id}]")
            return []

    @cached_property
    def streams(self) -> list[str]:
        result = []
        with get_db_session() as session:

            base_stmt = (
                sa.select(mbdata.models.URL, mbdata.models.Link, mbdata.models.LinkAttribute)
                .select_from(
                    sa.join(
                        sa.join(mbdata.models.URL, mbdata.models.LinkRecordingURL).join(mbdata.models.Recording),
                        sa.join(mbdata.models.Link, mbdata.models.LinkAttribute),
                        isouter=True
                    ))
            )
            stmt = base_stmt.where(mbdata.models.LinkRecordingURL.recording_id == str(self._db_id))

            res: sa.ChunkedIteratorResult = session.execute(stmt)

            if res.raw.rowcount == 0:
                _logger.debug(f"Also looking for streams of siblings")

                siblings = [str(s.id) for s in self.siblings]

                stmt = base_stmt.where(mbdata.models.Recording.gid.in_(siblings))
                res: list[mbdata.models.URL, mbdata.models.Link, mbdata.models.LinkAttribute] = session.execute(stmt)

            for (url, link, la) in res:
                if la is not None:
                    if la.attribute_type_id == 582:  # video
                        continue
                if url.url not in result:
                    result.append(url.url)

        return result

    @cached_property
    def spotify_id(self) -> str | None:
        spotify_id_regex = r'open\.spotify\.com/\w+/([0-9A-Za-z]+)'
        for url in self.streams:
            match = re.search(spotify_id_regex, url)
            if match:
                id_ = match.group(1)
                if id_:
                    return id_
        return None

    def __repr__(self):
        s_date = f" {self.first_release_date}" if self.first_release_date is not None else ""
        return f"Recording:  {self.artist_credit_phrase} - {self.title}{s_date} [{self.id}] " + (
            "/".join(self.performance_type) if len(self.performance_type) > 0 else "")

    def __eq__(self, other):
        if isinstance(other, Recording):
            return self.id == other.id
        else:
            return False

    def __lt__(self, other):
        if isinstance(other, Recording):

            if self.first_release_date is not None:
                if other.first_release_date is not None:
                    return self.first_release_date < other.first_release_date
                else:
                    return True
            else:
                return False

    def is_sane(self, artist_query: str, title_query: str, cut_off=70) -> bool:
        artist_sane = any([artist.is_sane(artist_query) for artist in self.artists])

        title_ratio = rapidfuzz.process.extractOne(
            flatten_title(recording_name=title_query),
            [flatten_title(recording_name=self.title)] + [flatten_title(recording_name=a) for a in
                                                          self.aliases],
            processor=rapidfuzz.utils.default_process
        )[1]

        if not artist_sane:
            _logger.debug(f"{self} is not a sane candidate for artist {artist_query}")
        elif title_ratio < cut_off:
            _logger.debug(f"{self} is not a sane candidate for title {title_query}")
        else:
            return True

    @property
    def url(self) -> str:
        return f"https://musicbrainz.org/recording/{self.id}"

    def __hash__(self):
        return hash(self.id)


class Medium(MusicBrainzObject):

    def __init__(self,
                 in_obj: mbdata.models.Medium) -> None:
        with get_db_session() as session:
            if isinstance(in_obj, mbdata.models.Medium):
                m: mbdata.models.Medium = session.merge(in_obj)

            self._db_id: int = m.id
            self.title: str = m.name
            self.position: int = m.position
            self._release_id: ReleaseID = ReleaseID(str(m.release.gid))
            self._track_ids: list[TrackID] = [TrackID(str(t.gid)) for t in m.tracks]
            self.track_count = m.track_count
            self.format = m.format.name

    @cached_property
    def release(self) -> Release:
        return get_release(self._release_id)

    @cached_property
    def tracks(self) -> list["Track"]:
        return [get_track(t) for t in self._track_ids]

    def __repr__(self):
        return (
                f"Medium: {self.release.artist_credit_phrase} - {self.release.title}"
                + (f" - {self.title}" if self.title else "")
        )


class Track(MusicBrainzObject):

    def __init__(self,
                 in_obj: TrackID | mbdata.models.Track | str) -> None:
        with get_db_session() as session:
            if isinstance(in_obj, mbdata.models.Track):
                tr: mbdata.models.Track = session.merge(in_obj)
            else:
                if isinstance(in_obj, str):
                    in_obj = TrackID(in_obj)
                stmt = sa.select(mbdata.models.Track).where(mbdata.models.Track.gid == str(in_obj))
                tr: mbdata.models.Track = session.scalar(stmt)

            self.id: TrackID = TrackID(str(tr.gid))
            self._db_id: int = tr.id
            self.artists = [get_artist(ArtistID(str(a.artist.gid))) for a in tr.artist_credit.artists]
            self.title: str = tr.name
            self.artist_credit_phrase: str = tr.artist_credit.name
            self.position: int = tr.position
            self.number: str = tr.number
            self.length: int = tr.length
            self.medium: Medium = get_medium(tr.medium)

            self._recording_id: RecordingID = RecordingID(str(tr.recording.gid))

    @cached_property
    def recording(self) -> Recording:
        return get_recording(self._recording_id)

    @cached_property
    def release(self) -> Release:
        return self.medium.release

    def __repr__(self):
        return f"Track {self.position}/{self.medium.track_count} of {self.release.artist_credit_phrase} - {self.release.title} / {self.recording.artist_credit_phrase} - {self.recording.title}"


class Work(MusicBrainzObject):
    def __init__(self,
                 in_obj: WorkID | mbdata.models.Work | str) -> None:
        with get_db_session() as session:
            if isinstance(in_obj, mbdata.models.Work):
                w: mbdata.models.Work = session.merge(in_obj)
            else:
                if isinstance(in_obj, str):
                    in_obj = WorkID(in_obj)
                stmt = sa.select(mbdata.models.Work).where(mbdata.models.Work.gid == str(in_obj))
                w: mbdata.models.Work = session.scalar(stmt)

            self.id: WorkID = WorkID(str(w.gid))
            self._db_id: int = w.id
            self.title: str = w.name
            self.disambiguation: str = w.comment
            self.type: str = w.type.name

    @cached_property
    def performances(self) -> dict[str, list[Recording]]:
        results = {'all': [], 'no-attr': []}
        with get_db_session() as session:

            stmt = (
                sa.select(mbdata.models.Recording, mbdata.models.LinkAttribute)
                .select_from(
                    sa.join(
                        sa.join(mbdata.models.LinkRecordingWork, mbdata.models.Recording),
                        sa.join(mbdata.models.LinkAttribute, mbdata.models.Link),
                        isouter=True
                    )
                )
                .where(mbdata.models.LinkRecordingWork.entity1_id == str(self._db_id))
            )

            res = session.execute(stmt)

            for (r, la) in res:
                rec: Recording = get_recording(r)
                if rec not in results['all']:
                    results['all'].append(rec)

                if la is None:
                    results['no-attr'].append(rec)
                else:
                    if la.attribute_type.name in results.keys():
                        results[la.attribute_type.name].append(rec)
                    else:
                        results[la.attribute_type.name] = [rec]

        return results

    def __repr__(self):
        return f"Work:  {self.title}  [{self.id}]"

    def __eq__(self, other):
        if isinstance(other, Work):
            return self.id == other.id
        else:
            return False

    @property
    def url(self) -> str:
        return f"https://musicbrainz.org/work/{self.id}"

    def __hash__(self):
        return hash(self.id)


_object_cache = {}


def clear_object_cache():
    global _object_cache
    _object_cache = {}


def get_artist(in_obj: ArtistID | str | mbdata.models.Artist) -> Artist:
    global _object_cache
    if isinstance(in_obj, mbdata.models.Artist):
        if ArtistID(str(in_obj.gid)) in _object_cache.keys():
            return _object_cache[ArtistID(str(in_obj.gid))]
        else:
            a = Artist(in_obj)
            _object_cache[ArtistID(str(in_obj.gid))] = a
            return a
    if isinstance(in_obj, str):
        in_obj = ArtistID(in_obj)

    if in_obj in _object_cache.keys():
        return _object_cache[in_obj]
    else:
        a = Artist(in_obj)
        _object_cache[a.id] = a
        return a


def get_release_group(in_obj: ReleaseGroupID | str | mbdata.models.ReleaseGroup) -> ReleaseGroup:
    global _object_cache
    if isinstance(in_obj, mbdata.models.ReleaseGroup):
        if ReleaseGroupID(str(in_obj.gid)) in _object_cache.keys():
            return _object_cache[ReleaseGroupID(str(in_obj.gid))]
        else:
            a = ReleaseGroup(in_obj)
            _object_cache[ReleaseGroupID(str(in_obj.gid))] = a
            return a
    if isinstance(in_obj, str):
        in_obj = ReleaseGroupID(in_obj)

    if in_obj in _object_cache.keys():
        return _object_cache[in_obj]
    else:
        a = ReleaseGroup(in_obj)
        _object_cache[a.id] = a
        return a


def get_release(in_obj: ReleaseID | str | mbdata.models.Release) -> Release:
    global _object_cache
    if isinstance(in_obj, mbdata.models.Release):
        if ReleaseID(str(in_obj.gid)) in _object_cache.keys():
            return _object_cache[ReleaseID(str(in_obj.gid))]
        else:
            a = Release(in_obj)
            _object_cache[ReleaseID(str(in_obj.gid))] = a
            return a
    if isinstance(in_obj, str):
        in_obj = ReleaseID(in_obj)

    if in_obj in _object_cache.keys():
        return _object_cache[in_obj]
    else:
        a = Release(in_obj)
        _object_cache[a.id] = a
        return a


def get_recording(in_obj: RecordingID | str | mbdata.models.Recording) -> Recording:
    global _object_cache
    if isinstance(in_obj, mbdata.models.Recording):
        if RecordingID(str(in_obj.gid)) in _object_cache.keys():
            return _object_cache[RecordingID(str(in_obj.gid))]
        else:
            a = Recording(in_obj)
            _object_cache[RecordingID(str(in_obj.gid))] = a
            return a
    if isinstance(in_obj, str):
        in_obj = RecordingID(in_obj)

    if in_obj in _object_cache.keys():
        return _object_cache[in_obj]
    else:
        a = Recording(in_obj)
        _object_cache[a.id] = a
        return a


def get_track(in_obj: TrackID | str | mbdata.models.Track) -> Track:
    global _object_cache
    if isinstance(in_obj, mbdata.models.Track):
        if TrackID(str(in_obj.gid)) in _object_cache.keys():
            return _object_cache[TrackID(str(in_obj.gid))]
        else:
            a = Track(in_obj)
            _object_cache[TrackID(str(in_obj.gid))] = a
            return a
    if isinstance(in_obj, str):
        in_obj = TrackID(in_obj)

    if in_obj in _object_cache.keys():
        return _object_cache[in_obj]
    else:
        a = Track(in_obj)
        _object_cache[a.id] = a
        return a


def get_work(in_obj: WorkID | str | mbdata.models.Work) -> Work:
    global _object_cache
    if isinstance(in_obj, mbdata.models.Work):
        if WorkID(str(in_obj.gid)) in _object_cache.keys():
            return _object_cache[WorkID(str(in_obj.gid))]
        else:
            a = Work(in_obj)
            _object_cache[WorkID(str(in_obj.gid))] = a
            return a
    if isinstance(in_obj, str):
        in_obj = WorkID(in_obj)

    if in_obj in _object_cache.keys():
        return _object_cache[in_obj]
    else:
        a = Work(in_obj)
        _object_cache[a.id] = a
        return a


def get_medium(in_obj: mbdata.models.Medium) -> Medium:
    global _object_cache
    if in_obj is not None:
        if in_obj.id in _object_cache.keys():
            return _object_cache[in_obj.id]
        else:
            a = Medium(in_obj)
            _object_cache[in_obj.id] = a
            return a
    else:
        raise MBApiError("No parameters given")


_musicbrainzngs_configured: bool = False


def configure_musicbrainz_api(
        app: str = _DEFAULT_APP,
        version: str = _DEFAULT_VERSION,
        contact: str = _DEFAULT_CONTACT,
        api_url: str = _DEFAULT_API_URL,
        use_https: bool = _DEFAULT_HTTPS,
        rate_limit: bool = _DEFAULT_RATE_LIMIT,
):
    _logger.debug(
        f"Configuring MusicBrainz API access via 'http{'s' if use_https else ''}://{api_url}' with rate limiting {'enabled' if rate_limit else 'disabled'}.")
    musicbrainzngs.set_hostname(api_url, use_https=use_https)
    musicbrainzngs.set_rate_limit(rate_limit)
    musicbrainzngs.set_useragent(app=app, version=version, contact=contact)


def search_artists(artist_query: str, cut_off: int = 90) -> list["Artist"]:
    _logger.debug(f"Searching for artist '{artist_query}' from MusicBrainz API")

    search_params = {}

    try:
        result = []
        artist_split = split_artist(artist_query)
        _logger.debug(f"Split artist query to: {artist_split}")
        for artist_split_query in artist_split:
            response = musicbrainzngs.search_artists(artist=artist_split_query, **search_params)

            for r in response["artist-list"]:
                score = int(r["ext:score"])
                if score > cut_off:
                    artist_id = ArtistID(r["id"])
                    if artist_id not in result and artist_id not in [VA_ARTIST_ID, UNKNOWN_ARTIST_ID]:
                        result.append(artist_id)
        result = [get_artist(x) for x in result]
        result = [x for x in result if x.is_sane(artist_query, cut_off)]
        result = sorted(result, reverse=True)
        _logger.debug(f"Search gave us {len(result)} results above cutoff threshold")
        return result
    except musicbrainzngs.WebServiceError as ex:
        raise MBApiError("Could not get result from musicbrainz_wrapper API") from ex


def search_recording(
        artist_query: str,
        title_query: str,
        date: datetime.date = None,
        cut_off: int = 90) -> list["Recording"]:
    _logger.debug(f"Searching for recording '{artist_query}' - '{title_query}' from MusicBrainz API")

    result_ids = []

    search_params = {
        "artist": artist_query,
        "alias": title_query,  # or "recording"
        "limit": 100,
        "status": str(ReleaseStatus.OFFICIAL),
        "video": False,
        "strict": True
    }

    try:

        offset = 0
        fetched = None
        while fetched is None or fetched >= 100:
            fetch_result = musicbrainzngs.search_recordings(**search_params, offset=offset)
            fetched = len(fetch_result["recording-list"])
            for r in fetch_result["recording-list"]:
                score = int(r["ext:score"])
                if score > cut_off:
                    result_ids.append((RecordingID(r["id"]), score))
                else:
                    fetched = 0
                    break
            offset = offset + fetched

        _logger.debug(f"Search gave us {len(result_ids)} results above cutoff threshold")

        result = []
        for rid, score in result_ids:
            try:
                recording = get_recording(rid)
                if recording.is_sane(artist_query, title_query):
                    result.append((recording, score))
            except MBApiError as ex:
                _logger.warning(f"Could not get recording {str(rid)}")

        result = sorted(result, key=lambda x: x[1], reverse=True)
        result = [x[0] for x in result]
        return result
    except musicbrainzngs.WebServiceError as ex:
        raise MBApiError("Could not get result from musicbrainz_wrapper API") from ex


def typesense_lookup(artist_name, recording_name):
    hits = _do_typesense_lookup(artist_name, recording_name)

    output = []
    for hit in hits:
        hit['artists'] = [get_artist(x) for x in hit['artist_ids']]
        hit['release'] = get_release(hit['release_id'])
        hit['recording'] = get_recording(hit['recording_id'])
        hit['release_group'] = hit['release'].release_group
        output.append(hit)
    return output


class SearchType(enum.StrEnum):
    CANONICAL = "canonical"
    STUDIO_ALBUM = "studio_album"
    SINGLE = "single"
    SOUNDTRACK = "soundtrack"
    EP = "ep"
    ALL = "all"


def select_best_candidate(candidates: Mapping[str, Sequence[tuple[ReleaseGroup, Sequence[Recording]]]]) -> tuple[
    ReleaseGroup, Recording]:
    # {
    #     "studio_albums": sorted(albums),
    #     "eps": sorted(eps),
    #     "soundtracks": sorted(soundtracks),
    #     "singles": sorted(singles)
    # }

    if len(candidates["studio_albums"]) > 0:
        if len(candidates["soundtracks"]) > 0:
            if candidates["studio_albums"][0][0] < candidates["soundtracks"][0][0]:
                _logger.debug(f"Choosing oldest studio album over potential soundtrack")
                return candidates["studio_albums"][0][0], candidates["studio_albums"][0][1][0],
            else:
                _logger.debug(f"Choosing soundtrack that is older than oldest studio album")
                return candidates["soundtracks"][0][0], candidates["soundtracks"][0][1][0],
        else:
            _logger.debug(f"Choosing oldest studio album")
            return candidates["studio_albums"][0][0], candidates["studio_albums"][0][1][0],
    elif len(candidates["eps"]) > 0:
        if len(candidates["soundtracks"]) > 0:
            if candidates["eps"][0][0] < candidates["soundtracks"][0][0]:
                _logger.debug(f"Choosing oldest EP over potential soundtrack")
                return candidates["eps"][0][0], candidates["eps"][0][1][0],
            else:
                _logger.debug(f"Choosing soundtrack that is older than oldest EP")
                return candidates["soundtracks"][0][0], candidates["soundtracks"][0][1][0],
        else:
            _logger.debug(f"Choosing oldest EP")
            return candidates["eps"][0][0], candidates["eps"][0][1][0],
    elif len(candidates["soundtracks"]) > 0:
        _logger.debug(f"Choosing oldest soundtrack")
        return candidates["soundtracks"][0][0], candidates["soundtracks"][0][1][0],
    elif "singles" not in candidates.keys():
        raise NotFoundError("Expecting to look at singles, but they were not fetched.")
    elif len(candidates["singles"]) > 0:
        return candidates["singles"][0][0], candidates["singles"][0][1][0],
    else:
        raise NotFoundError("Somehow we didn't get any viable candidates")


def search_canonical_release(
        artist_query: str,
        title_query: str,
) -> dict[str, MusicBrainzObject] | None:
    _logger.debug("Doing a lookup for canonical release")
    canonical_hits = typesense_lookup(artist_query, title_query)
    if len(canonical_hits) > 0:
        _logger.info("Found canonical release according to MusicBrainz Canonical dataset")
        rg: ReleaseGroup = canonical_hits[0]['release_group']
        recording: Recording = canonical_hits[0]['recording']
        release: Release = canonical_hits[0]['release']
        track: Track = find_track_for_release_recording(release, recording)
        return {"release_group": rg,
                "recording": recording,
                "release": release,
                "track": track
                }
    else:
        return None


def _search_release_group_by_recording_ids(
        recording_ids: RecordingID | Sequence[RecordingID],
        search_type: SearchType,
        use_siblings: bool = True,
        cut_off: int = None
) -> dict[str, MusicBrainzObject] | None:
    if cut_off is None:
        cut_off = 97

    # get actual MB objects
    if isinstance(recording_ids, RecordingID):
        recordings = [get_recording(recording_ids)]
    else:
        recordings = [get_recording(x) for x in recording_ids]

    # also search for recording siblings
    if use_siblings:
        for recording in recordings:
            for sibling in recording.siblings:
                if sibling not in recordings:
                    recordings.append(sibling)

    match search_type:
        case SearchType.CANONICAL:
            return None
        case SearchType.STUDIO_ALBUM:
            search_field = "studio_albums"
        case SearchType.SINGLE:
            search_field = "singles"
        case SearchType.SOUNDTRACK:
            search_field = "soundtracks"
        case SearchType.EP:
            search_field = "eps"
        case SearchType.ALL | _:
            search_field = "release_groups"

    # find the actual release groups
    found_rgs = []
    for recording in recordings:
        for artist in recording.artists:
            for rg in getattr(artist, search_field):
                if recording in rg:
                    track, release = find_track_release_for_release_group_recording(rg, recording)
                    if (rg, recording, release, track) not in found_rgs:
                        found_rgs.append((rg, recording, release, track))

    found_rgs = sorted(found_rgs, key=lambda x: x[2])
    if len(found_rgs) > 0:
        _logger.debug(f"Found {found_rgs[0][3]}")
        return {
            "release_group": found_rgs[0][0],
            "recording": found_rgs[0][1],
            "release": found_rgs[0][2],
            "track": found_rgs[0][3]
        }
    else:
        return None


def search_studio_albums_by_recording_ids(
        recording_ids: RecordingID | Sequence[RecordingID],
        use_siblings: bool = True,
        cut_off: int = None
) -> dict[str, MusicBrainzObject] | None:
    return _search_release_group_by_recording_ids(
        recording_ids=recording_ids,
        search_type=SearchType.STUDIO_ALBUM,
        use_siblings=use_siblings,
        cut_off=cut_off
    )


def search_soundtracks_by_recording_ids(
        recording_ids: RecordingID | Sequence[RecordingID],
        use_siblings: bool = True,
        cut_off: int = None
) -> dict[str, MusicBrainzObject] | None:
    return _search_release_group_by_recording_ids(
        recording_ids=recording_ids,
        search_type=SearchType.SOUNDTRACK,
        use_siblings=use_siblings,
        cut_off=cut_off
    )


def search_eps_by_recording_ids(
        recording_ids: RecordingID | Sequence[RecordingID],
        use_siblings: bool = True,
        cut_off: int = None
) -> dict[str, MusicBrainzObject] | None:
    return _search_release_group_by_recording_ids(
        recording_ids=recording_ids,
        search_type=SearchType.EP,
        use_siblings=use_siblings,
        cut_off=cut_off
    )


def search_singles_by_recording_ids(
        recording_ids: RecordingID | Sequence[RecordingID],
        use_siblings: bool = True,
        cut_off: int = None
) -> dict[str, MusicBrainzObject] | None:
    return _search_release_group_by_recording_ids(
        recording_ids=recording_ids,
        search_type=SearchType.SINGLE,
        use_siblings=use_siblings,
        cut_off=cut_off
    )


def search_release_groups_by_recording_ids(
        recording_ids: RecordingID | Sequence[RecordingID],
        use_siblings: bool = True,
        cut_off: int = None
) -> dict[str, MusicBrainzObject] | None:
    return _search_release_group_by_recording_ids(
        recording_ids=recording_ids,
        search_type=SearchType.ALL,
        use_siblings=use_siblings,
        cut_off=cut_off
    )


def search_by_recording_id(
        recording_ids: RecordingID | Sequence[RecordingID],
        use_siblings: bool = True,
        cut_off: int = None

) -> dict[SearchType, dict[str, MusicBrainzObject]]:
    results = {
        search_type: _search_release_group_by_recording_ids(
            recording_ids=recording_ids,
            search_type=SearchType(search_type),
            use_siblings=use_siblings,
            cut_off=cut_off) for search_type in SearchType}

    return {k: v for k, v in results.items() if v is not None}


def _recording_id_from_fingerprint(file: pathlib.Path, cut_off: int = None) -> list[RecordingID]:
    if cut_off is None:
        cut_off = 97

    recording_ids = []
    try:
        for score, recording_id, title, artist in acoustid.match(ACOUSTID_APIKEY, str(file)):
            if score > cut_off / 100:
                recording_ids.append(RecordingID(recording_id))
        return recording_ids
    except acoustid.FingerprintGenerationError as ex:
        _logger.error(f"Could not compute fingerprint for file '{file}'")
        raise MBApiError(f"Could not compute fingerprint for file '{file}'") from ex
    except acoustid.WebServiceError as ex:
        _logger.error("Could not obtain Acoustid fingerprint from webservice")
        raise MBApiError("Could not obtain Acoustid fingerprint from webservice") from ex


def search_fingerprint(file: pathlib.Path, cut_off: int = None) \
        -> dict[SearchType, dict[str, MusicBrainzObject]]:
    recording_ids = _recording_id_from_fingerprint(file=file, cut_off=cut_off)
    return search_by_recording_id(recording_ids)


def search_fingerprint_by_type(
        file: pathlib.Path,
        search_type: SearchType,
        use_siblings: bool = True,
        cut_off: int = None) -> dict[str, MusicBrainzObject]:
    recording_ids = _recording_id_from_fingerprint(file=file, cut_off=cut_off)

    return _search_release_group_by_recording_ids(
        recording_ids=recording_ids,
        search_type=search_type,
        use_siblings=use_siblings,
        cut_off=cut_off
    )


def search_name(artist_query: str, title_query: str, cut_off: int = None) \
        -> dict[SearchType, dict[str, MusicBrainzObject]]:
    if cut_off is None:
        cut_off = 90

    canonical = search_canonical_release(artist_query=artist_query, title_query=title_query)

    songs_found = search_recording(artist_query=artist_query, title_query=title_query, cut_off=cut_off)
    recording_ids = [recording.id for recording in songs_found if recording.is_sane(artist_query, title_query)]
    result = search_by_recording_id(recording_ids)
    result[SearchType.CANONICAL] = canonical

    return result


def search_name_by_type(
        artist_query: str,
        title_query: str,
        search_type: SearchType,
        use_siblings: bool = True,
        cut_off: int = None) -> dict[str, MusicBrainzObject]:
    songs_found = search_recording(
        artist_query=artist_query,
        title_query=title_query,
        cut_off=cut_off)

    recording_ids = [recording.id for recording in songs_found if recording.is_sane(artist_query, title_query)]

    return _search_release_group_by_recording_ids(
        recording_ids=recording_ids,
        search_type=search_type,
        use_siblings=use_siblings,
        cut_off=cut_off
    )


def find_best_release_group(
        artist_query: str,
        title_query: str,
        canonical: bool = True,
        lookup_singles: bool = True,

        date: int | datetime.date = None,
        file: pathlib.Path = None,
        cut_off: int = 90,
) -> tuple[ReleaseGroup, Recording, Release, Track] | tuple[None, None, None, None]:
    try:

        if isinstance(date, int):
            date = datetime.date(date, 1, 1)

        if canonical:
            _logger.debug("Doing a lookup for canonical release")
            canonical_hits = typesense_lookup(artist_query, title_query)
            if len(canonical_hits) > 0:
                _logger.info("Found canonical release according to MusicBrainz Canonical dataset")
                rg: ReleaseGroup = canonical_hits[0]['release_group']
                recording: Recording = canonical_hits[0]['recording']
                release: Release = canonical_hits[0]['release']
                track: Track = find_track_for_release_recording(release, recording)
                return rg, recording, release, track
            else:
                _logger.info("No canonical release found. Falling back to brute force search")

        candidates = find_best_release_group_by_search(artist_query, title_query, date, cut_off,
                                                       lookup_singles=lookup_singles)

        if sum([len(x) for x in candidates.values()]) == 0:
            _logger.debug(f"Could not find a result by searching. Falling back to exhaustive artist search.")
            candidates = find_best_release_group_by_artist(artist_query, title_query, cut_off,
                                                           lookup_singles=lookup_singles)

        if sum([len(x) for x in candidates.values()]) == 0:
            _logger.debug(f"Could not find a result by exhaustive artist search. Trying Acoustid lookup")

            if file is not None:
                candidates = find_best_release_group_by_fingerprint(file, artist_query, title_query, cut_off,
                                                                    lookup_singles=lookup_singles)

        if sum([len(x) for x in candidates.values()]) == 0:
            _logger.debug(f"Could not find any candidates for this file")
            return None, None, None, None

        # we now have a dict with potential release groups
        rg: ReleaseGroup
        recording: Recording
        rg, recording = select_best_candidate(candidates)
        track, release = find_track_release_for_release_group_recording(rg, recording)

        return rg, recording, release, track

    except MBApiError as ex:
        _logger.exception(ex)
        return None, None, None, None


def find_release_for_release_group_recording(rg: ReleaseGroup, recording: Recording) -> Release:
    found = None
    for r in rg.releases:
        if recording in r:
            return r


def find_track_for_release_recording(release: Release, recording: Recording) -> Track:
    for track in release.tracks:
        if track.recording == recording:
            return track


def find_track_release_for_release_group_recording(rg: ReleaseGroup, recording: Recording) -> tuple[Track, Release]:
    for r in rg.releases:
        for track in r.tracks:
            if track.recording == recording:
                return track, r


def find_best_release_group_by_recording_ids(
        recording_ids: Sequence[RecordingID],

        artist_query: str = None,
        title_query: str = None,
        cut_off: int = 97,
        lookup_singles: bool = True,
) -> dict[str, Sequence[tuple[ReleaseGroup, Sequence[Recording]]]]:
    result = []

    for recording_id in recording_ids:
        recording: Recording = get_recording(recording_id)
        if artist_query is not None and title_query is not None:
            if recording.is_sane(artist_query=artist_query, title_query=title_query):
                result.append(recording)
        else:
            result.append(recording)

    albums = []
    eps = []
    soundtracks = []
    singles = []

    choice: ReleaseGroup
    for recording in result:
        for artist in recording.artists:
            for rg in artist.soundtracks:
                for recording2 in rg.recordings:
                    ratio = rapidfuzz.fuzz.WRatio(
                        flatten_title(recording.artist_credit_phrase, recording.title),
                        flatten_title(recording2.artist_credit_phrase, recording2.title),
                        processor=rapidfuzz.utils.default_process,
                        score_cutoff=cut_off
                    )
                    if ratio > cut_off and (rg, recording2) not in soundtracks:
                        soundtracks.append((rg, recording2))
                        _logger.debug(f"{ratio}%: {recording2} ")
            for rg in artist.studio_albums:
                for recording2 in rg.recordings:
                    ratio = rapidfuzz.fuzz.WRatio(
                        flatten_title(recording.artist_credit_phrase, recording.title),
                        flatten_title(recording2.artist_credit_phrase, recording2.title),
                        processor=rapidfuzz.utils.default_process,
                        score_cutoff=cut_off
                    )
                    if ratio > cut_off and (rg, recording2) not in albums:
                        albums.append((rg, recording2))
                        _logger.debug(f"{ratio}%: {recording2} ")
            for rg in artist.eps:
                for recording2 in rg.recordings:
                    ratio = rapidfuzz.fuzz.WRatio(
                        flatten_title(recording.artist_credit_phrase, recording.title),
                        flatten_title(recording2.artist_credit_phrase, recording2.title),
                        processor=rapidfuzz.utils.default_process,
                        score_cutoff=cut_off
                    )
                    if ratio > cut_off and (rg, recording2) not in eps:
                        eps.append((rg, recording2))
                        _logger.debug(f"{ratio}%: {recording2} ")

    # Do not load all singles if we don't need to
    if len(albums) + len(soundtracks) + len(eps) > 0:
        return {
            "studio_albums": _fold_sort_candidates(albums),
            "eps": _fold_sort_candidates(eps),
            "soundtracks": _fold_sort_candidates(soundtracks)
        }

    if not lookup_singles:
        _logger.warning(f"no album results, and --no-singles was provided")
        return {}

    _logger.debug(f"Did not find any albums; also looking at singles")
    for recording in result:
        for artist in recording.artists:
            for rg in artist.singles:
                for recording2 in rg.recordings:
                    ratio = rapidfuzz.fuzz.WRatio(
                        flatten_title(recording.artist_credit_phrase, recording.title),
                        flatten_title(recording2.artist_credit_phrase, recording2.title),
                        processor=rapidfuzz.utils.default_process,
                        score_cutoff=cut_off
                    )
                    if ratio > cut_off and (rg, recording2) not in singles:
                        singles.append((rg, recording2))
                        _logger.debug(f"{ratio}%: {recording2} ")

    return {
        "studio_albums": _fold_sort_candidates(albums),
        "eps": _fold_sort_candidates(eps),
        "soundtracks": _fold_sort_candidates(soundtracks),
        "singles": _fold_sort_candidates(singles)
    }


def find_best_release_group_by_fingerprint(
        file: pathlib.Path,

        artist_query: str = None,
        title_query: str = None,
        cut_off: int = None,
        lookup_singles: bool = True
) -> dict[str, Sequence[tuple[ReleaseGroup, Sequence[Recording]]]]:
    _logger.debug(f"Scanning Acoustid fingerprint for file {file}")

    recording_ids = []
    try:
        for score, recording_id, title, artist in acoustid.match(ACOUSTID_APIKEY, str(file)):
            if score > (cut_off if cut_off is not None else 97) / 100:
                recording_ids.append(recording_id)
    except acoustid.FingerprintGenerationError as ex:
        _logger.error(f"Could not compute fingerprint for file '{file}'")
        raise MBApiError(f"Could not compute fingerprint for file '{file}'") from ex
    except acoustid.WebServiceError as ex:
        _logger.error("Could not obtain Acoustid fingerprint from webservice")
        raise MBApiError("Could not obtain Acoustid fingerprint from webservice") from ex

    return find_best_release_group_by_recording_ids(
        recording_ids,
        artist_query,
        title_query,
        cut_off,
        lookup_singles,
    )


def find_best_release_group_by_artist(
        artist_query: str,

        title_query: str,
        year: int = None,
        cut_off: int = 90,
        lookup_singles: bool = True,
) -> dict[str, Sequence[tuple[ReleaseGroup, Sequence[Recording]]]]:
    artists_found = search_artists(artist_query, cut_off=cut_off)

    if len(artists_found) == 0:
        _logger.debug(f"Could not identify potential artists via artist search")
        return {}

    _logger.info("Identified the following potential artists: \n" + "\n".join([str(a) for a in artists_found]))

    artist: Artist
    recording: Recording
    me = flatten_title(artist_query, title_query)

    them = {}
    for artist in artists_found:
        for rg in artist.soundtracks:
            for recording in rg.recordings:
                if recording.is_sane(artist_query=artist_query, title_query=title_query) and (
                        rg, recording) not in them.keys():
                    them[(rg, recording)] = flatten_title(recording.artist_credit_phrase, recording.title)
    result = rapidfuzz.process.extract(me, them, limit=None, score_cutoff=90, processor=rapidfuzz.utils.default_process)
    soundtracks = [x[2] for x in result]

    them = {}
    for artist in artists_found:
        for rg in artist.studio_albums:
            for recording in rg.recordings:
                if recording.is_sane(artist_query=artist_query, title_query=title_query) and (
                        rg, recording) not in them.keys():
                    them[(rg, recording)] = flatten_title(recording.artist_credit_phrase, recording.title)
    result = rapidfuzz.process.extract(me, them, limit=None, score_cutoff=90, processor=rapidfuzz.utils.default_process)
    albums = [x[2] for x in result]

    them = {}
    for artist in artists_found:
        for rg in artist.eps:
            for recording in rg.recordings:
                if recording.is_sane(artist_query=artist_query, title_query=title_query) and (
                        rg, recording) not in them.keys():
                    them[(rg, recording)] = flatten_title(recording.artist_credit_phrase, recording.title)
    result = rapidfuzz.process.extract(me, them, limit=None, score_cutoff=90, processor=rapidfuzz.utils.default_process)
    eps = [x[2] for x in result]

    # Do not load all singles if we don't need to
    if len(albums) + len(soundtracks) + len(eps) > 0:
        return {
            "studio_albums": _fold_sort_candidates(albums),
            "eps": _fold_sort_candidates(eps),
            "soundtracks": _fold_sort_candidates(soundtracks)
        }

    if not lookup_singles:
        _logger.warning(f"no album results, and --no-singles was provided")
        return {}

    _logger.debug(f"Did not find any albums; also looking at singles")

    them = {}
    for artist in artists_found:
        for rg in artist.singles:
            for recording in rg.recordings:
                if recording.is_sane(artist_query=artist_query, title_query=title_query) and (
                        rg, recording) not in them.keys():
                    them[(rg, recording)] = flatten_title(recording.artist_credit_phrase, recording.title)
    result = rapidfuzz.process.extract(me, them, limit=None, score_cutoff=90, processor=rapidfuzz.utils.default_process)
    singles = [x[2] for x in result]

    return {
        "studio_albums": _fold_sort_candidates(albums),
        "eps": _fold_sort_candidates(eps),
        "soundtracks": _fold_sort_candidates(soundtracks),
        "singles": _fold_sort_candidates(singles)
    }


def find_best_release_group_by_search(
        artist_query: str,
        title_query: str,

        date: datetime.date = None,
        cut_off: int = None,
        lookup_singles: bool = True
) -> (
        dict[str, Sequence[tuple[ReleaseGroup, Sequence[Recording]]]]
):
    # First do a lookup for the song via a search query
    songs_found = search_recording(artist_query=artist_query, title_query=title_query, cut_off=cut_off,
                                   date=date)

    _logger.info(f"Found {len(songs_found)} recordings in search")
    if len(songs_found) == 0:
        return {}

    _logger.info(f"First {len(songs_found) if len(songs_found) < 5 else 5} recordings are\n" + "\n".join(
        str(x) for x in songs_found[0:10]))
    recording: Recording

    # expand with siblings
    songs_found = [id for x in songs_found for id in x.siblings]

    _logger.info(f"Found {len(songs_found)} sibling recordings")
    if len(songs_found) == 0:
        return {}

    albums = []
    eps = []
    soundtracks = []
    singles = []

    for recording in songs_found:
        for artist in recording.artists:
            for album in artist.studio_albums:
                if recording.is_sane(artist_query=artist_query,
                                     title_query=title_query) and recording in album and (
                        album, recording) not in albums:
                    albums.append((album, recording))
            for ep in artist.eps:
                if recording.is_sane(artist_query=artist_query,
                                     title_query=title_query) and recording in ep and (ep, recording) not in eps:
                    eps.append((ep, recording))
            for soundtrack in artist.soundtracks:
                if recording.is_sane(artist_query=artist_query,
                                     title_query=title_query) and recording in soundtrack and (
                        soundtrack, recording) not in soundtracks:
                    soundtracks.append((soundtrack, recording))

    _logger.info(f"Found {len(albums)} potential albums in search")
    _logger.info(f"Found {len(eps)}  potential eps in search")
    _logger.info(f"Found {len(soundtracks)} soundtracks in search")

    # Do not load all singles if we don't need to
    if len(albums) + len(soundtracks) + len(eps) > 0:
        return {
            "studio_albums": _fold_sort_candidates(albums),
            "eps": _fold_sort_candidates(eps),
            "soundtracks": _fold_sort_candidates(soundtracks)
        }

    if not lookup_singles:
        _logger.warning(f"no album results, and --no-singles was provided")
        return {}

    _logger.debug(f"Did not find any albums; also looking at singles")

    for recording in songs_found:
        for artist in recording.artists:
            for single in artist.singles:
                if recording.is_sane(artist_query=artist_query,
                                     title_query=title_query) and recording in single and (
                        single, recording) not in singles:
                    singles.append((single, recording))

    _logger.info(f"Found {len(singles)} potential singles in search")

    return {
        "studio_albums": _fold_sort_candidates(albums),
        "eps": _fold_sort_candidates(eps),
        "soundtracks": _fold_sort_candidates(soundtracks),
        "singles": _fold_sort_candidates(singles)
    }


_url: urllib3.util.Url = urllib3.util.parse_url("http://musicbrainz.int.liesdonk.nl:8108")
_api_key: str = "xyz"

_search_field: str = "combined"
_collection: str = "musicbrainz"

_client: typesense.Client | None = None


def configure_typesense(url: urllib3.util.Url = None, api_key: str = None, collection: str = None,
                        search_field: str = None):
    global _url, _api_key, _collection, _search_field
    if url is not None:
        _logger.info(f"Now configured to access typesense at {url}")
        _url = url

    if collection is not None:
        _logger.info(f"Now configured to read typesense collection '{collection}'")
        _collection = collection

    if search_field is not None:
        _logger.info(f"Now configured to search typesense field {search_field}")
        _search_field = search_field

    if api_key is not None:
        _api_key = api_key


def get_client():
    global _client
    if _client is None:
        _client = typesense.Client({
            'nodes': [{
                'host': _url.host,
                'port': _url.port,
                'protocol': _url.scheme,
            }],
            'api_key': _api_key,
            'connection_timeout_seconds': 1000000
        })
        _logger.debug("Connected Typesense client")
    return _client


def _do_typesense_lookup(artist_name, recording_name):
    """ Perform a lookup on the typsense index """

    client = get_client()
    query = flatten_title(artist_name, recording_name)
    search_parameters = {'q': query, 'query_by': _search_field, 'prefix': 'no', 'num_typos': 5}

    _logger.debug(f"Search typesense collection {_collection} for '{_search_field}'~='{query}'.")
    hits = client.collections[_collection].documents.search(search_parameters)

    output = []
    for hit in hits['hits']:
        doc = hit['document']
        acn = doc['artist_credit_name']
        artist_ids = doc['artist_mbids'].split(',')
        release_id = doc['release_mbid']

        recording_id = doc['recording_mbid']
        output.append(
            {
                'artist_credit_name': acn,
                'artist_ids': [ArtistID(x) for x in artist_ids],
                'release_id': ReleaseID(release_id),
                'recording_id': RecordingID(recording_id)
            }
        )

    return output
