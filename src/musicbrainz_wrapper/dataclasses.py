import datetime
from abc import ABC
from functools import cached_property, cache
from typing import Union
import logging
import sqlalchemy as sa
import sqlalchemy.orm as orm

import dateutil.parser
import rapidfuzz

from . import util
from .datatypes import ArtistID, ReleaseGroupID, ReleaseType, ReleaseID, RecordingID, ReleaseStatus, WorkID, TrackID, \
    MBID, PRIMARY_TYPES, SECONDARY_TYPES
from .db import get_db_session
from .exceptions import MBApiError, IncomparableError
from .api import MBApi

from .util import split_artist, flatten_title, parse_partial_date, CachedGenerator

import mbdata.models as mb_models

_logger = logging.getLogger(__name__)


class MusicBrainzObject(ABC):
    pass


class Artist(MusicBrainzObject):

    def __init__(self,
                 artist_id: ArtistID = None,
                 mb_artist: mb_models.Artist = None,
                 artist_db_id: int = None) -> None:
        with get_db_session() as session:
            if mb_artist is not None:
                session.add(mb_artist)
                a = mb_artist
            elif artist_db_id is not None:
                a = session.get(mb_models.Artist, artist_db_id)
            elif artist_id is not None:
                stmt = sa.select(mb_models.Artist).where(mb_models.Artist.gid == str(artist_id))
                a: mb_models.Artist = session.scalar(stmt)
            else:
                raise MBApiError("No parameters given")

            self.id: ArtistID = ArtistID(str(a.gid))
            self._db_id: int = a.id
            self.name: str = a.name
            self.artist_type: str = a.type.name if a.type is not None else None
            self.sort_name: str = a.sort_name
            self.disambiguation: str = a.comment

    @cached_property
    def aliases(self) -> list[str]:
        with get_db_session() as session:
            stmt = sa.select(mb_models.ArtistAlias).where(mb_models.ArtistAlias.artist.has(id=self._db_id))
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
        stmt = sa.select(mb_models.ReleaseGroup). \
            distinct(). \
            join(mb_models.ArtistReleaseGroup). \
            where(mb_models.ArtistReleaseGroup.artist.has(id=self._db_id)). \
            where(~mb_models.ArtistReleaseGroup.unofficial)

        # credited/contributing
        if credited:
            if not contributing:
                stmt = stmt.where(~mb_models.ArtistReleaseGroup.is_track_artist)
        else:
            if contributing:
                stmt = stmt.where(mb_models.ArtistReleaseGroup.is_track_artist)
            else:
                raise MBApiError("Query would result in no release groups")

        # primary type
        if primary_type is not None:
            stmt = stmt.where(mb_models.ArtistReleaseGroup.primary_type == PRIMARY_TYPES[primary_type])

        if secondary_types is not None:
            if len(secondary_types) > 0:
                types = [SECONDARY_TYPES[t] for t in secondary_types]
                where_clause = mb_models.ArtistReleaseGroup.secondary_types.contains(types)
                stmt = stmt.where(where_clause)
        else:
            stmt = stmt.where(mb_models.ArtistReleaseGroup.secondary_types.is_(None))

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
            result: list[mb_models.ReleaseGroup] = session.scalars(stmt).all()

        return [get_release_group(mb_release_group=rg) for rg in result]

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
                 release_group_id: ReleaseGroupID = None,
                 mb_release_group: mb_models.ReleaseGroup = None,
                 release_group_db_id: int = None) -> None:
        with get_db_session() as session:
            if mb_release_group is not None:
                session.add(mb_release_group)
                rg = mb_release_group
            elif release_group_db_id is not None:
                rg = session.get(mb_models.ReleaseGroup, release_group_db_id)
            elif release_group_id is not None:
                stmt = sa.select(mb_models.ReleaseGroup).where(mb_models.ReleaseGroup.gid == str(release_group_id))
                rg: mb_models.ReleaseGroup = session.scalar(stmt)
            else:
                raise MBApiError("No parameters given")

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
            stmt = sa.select(mb_models.ReleaseGroupMeta).where(mb_models.ReleaseGroupMeta.id == self._db_id)
            rgm: mb_models.ReleaseGroupMeta = session.scalar(stmt)

            return parse_partial_date(rgm.first_release_date)

    @cached_property
    def aliases(self) -> list[str]:
        result = [self.title]
        with get_db_session() as session:
            stmt = sa.select(mb_models.ReleaseGroupAlias).where(
                mb_models.ReleaseGroupAlias.release_group_id == self._db_id)
            rgas: list[mb_models.ReleaseGroupAlias] = session.scalars(stmt).all()

            for rga in rgas:
                if rga.name not in result:
                    result.append(rga.name)
        return result

    @cached_property
    def releases(self) -> list["Release"]:
        with get_db_session() as session:
            stmt = sa.select(mb_models.Release).where(mb_models.Release.release_group_id == self._db_id)
            releases: list[mb_models.Release] = session.scalars(stmt).all()

            return [Release(ReleaseID(str(release.gid))) for release in releases]

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
            util.flatten_title(artist_name=self.artist_credit_phrase),
            util - flatten_title(artist_name=artist_query),
            processor=rapidfuzz.utils.default_process,
            score_cutoff=cut_off
        )
        if artist_ratio < cut_off:
            _logger.warning(f"{self} is not a sane candidate for artist {artist_query}")
        title_ratio = rapidfuzz.process.extractOne(
            util.flatten_title(album_name=title_query),
            [util.flatten_title(album_name=self.title)] + [util.flatten_title(album_name=x) for x in self.aliases],
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
                 release_id: ReleaseID = None,
                 mb_release: mb_models.Release = None,
                 release_db_id: int = None) -> None:
        with get_db_session() as session:
            if mb_release is not None:
                session.add(mb_release)
                rel = mb_release
            elif release_db_id is not None:
                rel = session.get(mb_models.Release, release_db_id)
            elif release_id is not None:
                stmt = sa.select(mb_models.Release).where(mb_models.Release.gid == str(release_id))
                rel: mb_models.Release = session.scalar(stmt)
            else:
                raise MBApiError("No parameters given")

            self.id: ReleaseID = ReleaseID(str(rel.gid))
            self._db_id: int = rel.id
            self.artists = [get_artist(ArtistID(str(a.artist.gid))) for a in rel.artist_credit.artists]
            self.title: str = rel.name
            self.release_group_id: ReleaseGroupID = ReleaseGroupID(str(rel.release_group.gid))
            self.artist_credit_phrase: str = rel.artist_credit.name
            self.disambiguation: str = rel.comment
            self.first_release_date: datetime.date = parse_partial_date(
                rel.first_release.date) if rel.first_release is not None else None

    @cached_property
    def aliases(self) -> list[str]:
        result = [self.title]
        with get_db_session() as session:
            stmt = sa.select(mb_models.ReleaseAlias).where(
                mb_models.ReleaseAlias.release_id == self._db_id)
            ras: list[mb_models.ReleaseAlias] = session.scalars(stmt).all()

            for ra in ras:
                if ra.name not in result:
                    result.append(ra.name)
        return result

    @cached_property
    def url(self) -> str:
        return f"https://musicbrainz.org/release/{self.id}"

    @cached_property
    def release_group(self) -> ReleaseGroup:
        return ReleaseGroup(self.release_group_id)

    @cached_property
    def mediums(self) -> list["Medium"]:
        with get_db_session() as session:
            stmt = sa.select(mb_models.Medium).where(mb_models.Medium.release_id == str(self._db_id))
            ms: list[mb_models.Medium] = session.scalars(stmt).all()

            return [Medium(m) for m in ms]

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
            for r in t.recordings:
                if r not in result:
                    result.append(r)
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
                 recording_id: RecordingID = None,
                 mb_recording: mb_models.Recording = None,
                 recording_db_id: int = None) -> None:
        with get_db_session() as session:
            if mb_recording is not None:
                session.add(mb_recording)
                rec = mb_recording
            elif recording_db_id is not None:
                rec = session.get(mb_models.Recording, recording_db_id)
            elif recording_id is not None:
                stmt = sa.select(mb_models.Recording).where(mb_models.Recording.gid == str(recording_id))
                rec: mb_models.Recording = session.scalar(stmt)
            else:
                raise MBApiError("No parameters given")

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
            stmt = sa.select(mb_models.RecordingAlias).where(
                mb_models.RecordingAlias.recording_id == self._db_id)
            ras: list[mb_models.RecordingAlias] = session.scalars(stmt).all()

            for ra in ras:
                if ra.name not in result:
                    result.append(ra.name)
        return result

    @cached_property
    def performance_of_id(self) -> WorkID | None:
        if 'work-relation-list' not in self._json.keys():
            return None
        for a in self._json['work-relation-list']:
            if 'work' in a.keys() and a['direction'] == 'forward' and a['type'] == 'performance':
                return WorkID(a['work']['id'])

    @cached_property
    def performance_type(self) -> list[str]:
        l = []
        if 'work-relation-list' not in self._json.keys():
            return l

        for a in self._json['work-relation-list']:
            if 'work' in a.keys() and a['direction'] == 'forward' and a['type'] == 'performance':
                if 'attribute-list' in a.keys():
                    for att in a['attribute-list']:
                        if att not in l:
                            l.append(att)
        return l

    @cached_property
    def performance_of(self) -> Union["Work", None]:
        if self.performance_of_id is not None:
            return self._mb_api.get_work_by_id(self.performance_of_id)
        return None

    @cached_property
    def sibling_ids(self) -> list[RecordingID]:
        if self.performance_of is not None:
            if len(self.performance_type) == 0:
                return self.performance_of.performance_ids['no-attr']
            else:
                _logger.debug(f"Skipping sibling recordings because recording is a {'/'.join(self.performance_type)}")
        return []

    @cached_property
    def siblings(self) -> list["Recording"]:
        _logger.debug(
            f"Fetching {len(self.sibling_ids)} siblings for recording '{self.artist_credit_phrase}'- '{self.title}' [{self.id}]")
        return [self._mb_api.get_recording_by_id(x) for x in self.sibling_ids]

    def __repr__(self):
        s_date = f" {self.first_release_date}" if self.first_release_date is not None else ""
        return f"Recording:  {self.artist_credit_phrase} - {self.title}{s_date} [{self.id}]"

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
            f"{title_query}",
            [self.title] + self.aliases,
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

    def __init__(self, release: Release, json: dict,
                 search_cache: bool = True,
                 fetch_cache: bool = True):
        self._release: Release = release
        self._json: dict = json
        self._mb_api: MBApi = MBApi(search_cache=search_cache, fetch_cache=fetch_cache)

    @cached_property
    def title(self) -> str:
        return self._json["title"]

    @cached_property
    def position(self) -> str:
        return self._json["position"]

    @cached_property
    def release(self) -> Release:
        return self._release

    @cached_property
    def tracks(self) -> list["Track"]:
        return [Track(self, t) for t in self._json["track-list"]]

    def __repr__(self):
        return f"Medium: {self.release.artist_credit_phrase} - {self.release.title} - {self.title}"


class Track(MusicBrainzObject):

    def __init__(self,
                 track_id: TrackID = None,
                 mb_track: mb_models.Track = None,
                 track_db_id: int = None) -> None:
        with get_db_session() as session:
            if mb_track is not None:
                session.add(mb_track)
                tr = mb_track
            elif track_db_id is not None:
                tr = session.get(mb_models.Track, track_db_id)
            elif track_id is not None:
                stmt = sa.select(mb_models.Track).where(mb_models.Track.gid == str(track_id))
                tr: mb_models.Track = session.scalar(stmt)
            else:
                raise MBApiError("No parameters given")

            self.id: TrackID = TrackID(str(tr.gid))
            self._db_id: int = tr.id
            self.artists = [get_artist(ArtistID(str(a.artist.gid))) for a in tr.artist_credit.artists]
            self.title: str = tr.name
            self.artist_credit_phrase: str = tr.artist_credit.name
            self.position: str = tr.position
            self.number: str = tr.number

    @property
    def medium(self) -> Medium:
        return self._medium

    @cached_property
    def recording_id(self) -> RecordingID:
        return self._json["recording"]["id"]

    @cached_property
    def recording(self) -> Recording:
        return self._mb_api.get_recording_by_id(self.recording_id)

    @cached_property
    def release(self) -> Release:
        return self.medium.release

    def __repr__(self):
        return f"Track {self.position} of {self.recording.artist_credit_phrase} / {self.release.title} / {self.recording.title}"


class Work(MusicBrainzObject):
    def __init__(self, json: dict,
                 search_cache: bool = True,
                 fetch_cache: bool = True):
        self._json = json['work']
        self._id: WorkID = self._json['id']
        self._mb_api: MBApi = MBApi(search_cache=search_cache, fetch_cache=fetch_cache)

    @cached_property
    def id(self) -> WorkID:
        return self._id

    @cached_property
    def title(self) -> str:
        return self._json["title"]

    @cached_property
    def aliases(self) -> list[str]:
        if 'alias-list' in self._json:
            return [x['alias'] for x in self._json['alias-list']]
        return [self.title]

    @cached_property
    def type(self) -> str:
        return self._json["type"]

    @cached_property
    def performance_ids(self) -> dict[str, list[RecordingID]]:
        ids = {"all": [], "no-attr": []}
        if 'recording-relation-list' not in self._json.keys():
            return ids
        for a in self._json['recording-relation-list']:
            if 'recording' in a.keys() and a['direction'] == 'backward':
                if 'attribute-list' in a.keys():
                    for w in a['attribute-list']:
                        if w in ids.keys():
                            ids[w].append(RecordingID(a['recording']['id']))
                        else:
                            ids[w] = [RecordingID(a['recording']['id'])]
                else:
                    ids['no-attr'].append(RecordingID(a['recording']['id']))
                ids['all'].append(RecordingID(a['recording']['id']))
        return ids

    @cached_property
    def performances(self) -> dict[str, list["Recording"]]:
        result = {y: [self._mb_api.get_recording_by_id(x) for x in self.performance_ids[y]] for y in
                  self.performance_ids.keys()}
        return result

    @cached_property
    def disambiguation(self) -> str | None:
        return self._json["disambiguation"] if "disambiguation" in self._json.keys() else None

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


def get_artist(artist_id: ArtistID = None,
               mb_artist: mb_models.Artist = None) -> Artist:
    global _object_cache
    if mb_artist is not None:
        if ArtistID(mb_artist.gid) in _object_cache.keys():
            return _object_cache[ArtistID(mb_artist.gid)]
        else:
            a = Artist(mb_artist=mb_artist)
            _object_cache[ArtistID(mb_artist.gid)] = a
    elif artist_id is not None:
        if artist_id in _object_cache.keys():
            return _object_cache[ArtistID(mb_artist.gid)]
        else:
            a = Artist(artist_id=artist_id)
            _object_cache[ArtistID(mb_artist.gid)] = a
    else:
        raise MBApiError("No parameters given")


def get_release_group(release_group_id: ReleaseGroupID = None,
                      mb_release_group: mb_models.ReleaseGroup = None) -> ReleaseGroup:
    global _object_cache
    if mb_release_group is not None:
        if ReleaseGroupID(mb_release_group.gid) in _object_cache.keys():
            return _object_cache[ReleaseGroupID(mb_release_group.gid)]
        else:
            a = ReleaseGroup(mb_release_group=mb_release_group)
            _object_cache[ReleaseGroupID(mb_release_group.gid)] = a
    elif release_group_id is not None:
        if release_group_id in _object_cache.keys():
            return _object_cache[ReleaseGroupID(mb_release_group.gid)]
        else:
            a = get_release_group(release_group_id=release_group_id)
            _object_cache[ReleaseGroupID(mb_release_group.gid)] = a
    else:
        raise MBApiError("No parameters given")


def get_release(release_id: ReleaseID = None,
                mb_release: mb_models.Release = None) -> Release:
    global _object_cache
    if mb_release is not None:
        if ReleaseID(mb_release.gid) in _object_cache.keys():
            return _object_cache[ReleaseID(mb_release.gid)]
        else:
            a = Release(mb_release=mb_release)
            _object_cache[ReleaseID(mb_release.gid)] = a
    elif release_id is not None:
        if release_id in _object_cache.keys():
            return _object_cache[ReleaseID(mb_release.gid)]
        else:
            a = Release(release_id=release_id)
            _object_cache[ReleaseID(mb_release.gid)] = a
    else:
        raise MBApiError("No parameters given")


def get_recording(recording_id: RecordingID = None,
                  mb_recording: mb_models.Recording = None) -> Recording:
    global _object_cache
    if mb_recording is not None:
        if RecordingID(mb_recording.gid) in _object_cache.keys():
            return _object_cache[RecordingID(mb_recording.gid)]
        else:
            a = Recording(mb_recording=mb_recording)
            _object_cache[RecordingID(mb_recording.gid)] = a
    elif recording_id is not None:
        if recording_id in _object_cache.keys():
            return _object_cache[RecordingID(mb_recording.gid)]
        else:
            a = Recording(recording_id=recording_id)
            _object_cache[RecordingID(mb_recording.gid)] = a
    else:
        raise MBApiError("No parameters given")


def get_medium(mb_medium: mb_models.Medium = None) -> Medium:
    global _object_cache
    if mb_medium is not None:
        if mb_medium.id in _object_cache.keys():
            return _object_cache[mb_medium.id]
        else:
            a = Medium(mb_medium=mb_medium)
            _object_cache[mb_medium.id] = a

    else:
        raise MBApiError("No parameters given")


def get_track(track_id: TrackID = None,
              mb_track: mb_models.Track = None) -> Track:
    global _object_cache
    if mb_track is not None:
        if TrackID(mb_track.gid) in _object_cache.keys():
            return _object_cache[TrackID(mb_track.gid)]
        else:
            a = Track(mb_track=mb_track)
            _object_cache[TrackID(mb_track.gid)] = a
    elif track_id is not None:
        if track_id in _object_cache.keys():
            return _object_cache[TrackID(mb_track.gid)]
        else:
            a = Track(track_id=track_id)
            _object_cache[TrackID(mb_track.gid)] = a
    else:
        raise MBApiError("No parameters given")


def get_work(work_id: WorkID = None,
             mb_work: mb_models.Work = None) -> Work:
    global _object_cache
    if mb_work is not None:
        if WorkID(mb_work.gid) in _object_cache.keys():
            return _object_cache[WorkID(mb_work.gid)]
        else:
            a = Work(mb_work=mb_work)
            _object_cache[WorkID(mb_work.gid)] = a
    elif work_id is not None:
        if work_id in _object_cache.keys():
            return _object_cache[WorkID(mb_work.gid)]
        else:
            a = Work(work_id=work_id)
            _object_cache[WorkID(mb_work.gid)] = a
    else:
        raise MBApiError("No parameters given")
