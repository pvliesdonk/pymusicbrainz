import datetime
import re
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
    MBID, PRIMARY_TYPES, SECONDARY_TYPES, MediumID
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
                 in_obj: ArtistID | mb_models.Artist | str) -> None:
        with get_db_session() as session:
            if isinstance(in_obj, mb_models.Artist):
                a = session.merge(in_obj)
            else:
                if isinstance(in_obj, str):
                    in_obj = ArtistID(in_obj)
                stmt = sa.select(mb_models.Artist).where(mb_models.Artist.gid == str(in_obj))
                a: mb_models.Artist = session.scalar(stmt)

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
                 in_obj: ReleaseGroupID | mb_models.ReleaseGroup | str) -> None:
        with get_db_session() as session:
            if isinstance(in_obj, mb_models.ReleaseGroup):

                rg: mb_models.ReleaseGroup = session.merge(in_obj)
            else:
                if isinstance(in_obj, str):
                    in_obj = ReleaseGroupID(in_obj)
                stmt = sa.select(mb_models.ReleaseGroup).where(mb_models.ReleaseGroup.gid == str(in_obj))
                rg: mb_models.ReleaseGroup = session.scalar(stmt)

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
                 in_obj: ReleaseID | mb_models.Release | str) -> None:
        with get_db_session() as session:
            if isinstance(in_obj, mb_models.Release):
                rel: mb_models.Release = session.merge(in_obj)
            else:
                if isinstance(in_obj, str):
                    in_obj = ReleaseID(in_obj)
                stmt = sa.select(mb_models.Release).where(mb_models.Release.gid == str(in_obj))
                rel: mb_models.Release = session.scalar(stmt)

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
        return get_release_group(self._release_group_id)

    @cached_property
    def mediums(self) -> list["Medium"]:
        with get_db_session() as session:
            stmt = sa.select(mb_models.Medium).where(mb_models.Medium.release_id == str(self._db_id))
            ms: list[mb_models.Medium] = session.scalars(stmt).all()

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
                 in_obj: RecordingID | mb_models.Recording | str) -> None:
        with get_db_session() as session:
            if isinstance(in_obj, mb_models.Recording):
                rec: mb_models.Recording = session.merge(in_obj)
            else:
                if isinstance(in_obj, str):
                    in_obj = RecordingID(in_obj)
                stmt = sa.select(mb_models.Recording).where(mb_models.Recording.gid == str(in_obj))
                rec: mb_models.Recording = session.scalar(stmt)

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
    def performance_type(self) -> list[str]:
        p = self.performance_of
        return self.performance_type

    @cached_property
    def performance_of(self) -> "Work":
        with get_db_session() as session:
            stmt = sa.select(mb_models.LinkRecordingWork). \
                where(mb_models.LinkRecordingWork.entity0_id == str(self._db_id))
            res: mb_models.LinkRecordingWork = session.scalar(stmt)
            w = get_work(res.work)

            stmt = sa.select(mb_models.LinkAttribute). \
                where(mb_models.LinkAttribute.link == res.link)
            res2: list[mb_models.LinkAttribute] = session.scalars(stmt).all()
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
                sa.select(mb_models.URL, mb_models.Link, mb_models.LinkAttribute)
                .select_from(
                    sa.join(
                        sa.join(mb_models.URL, mb_models.LinkRecordingURL).join(mb_models.Recording),
                        sa.join(mb_models.Link, mb_models.LinkAttribute),
                        isouter=True
                    ))
                )
            stmt = base_stmt.where(mb_models.LinkRecordingURL.recording_id == str(self._db_id))

            res: sa.ChunkedIteratorResult = session.execute(stmt)

            if True: #res.raw.rowcount == 0:
                _logger.debug(f"Also looking for streams of siblings")

                siblings = [str(s.id) for s in self.siblings]

                stmt = base_stmt.where(mb_models.Recording.gid.in_(siblings))
                res: list[mb_models.URL, mb_models.Link, mb_models.LinkAttribute] = session.execute(stmt)

            for (url, link, la) in res:
                if la is not None:
                    if la.attribute_type_id == 582: # video
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
            util.flatten_title(recording_name=title_query),
            [util.flatten_title(recording_name=self.title)] + [util.flatten_title(recording_name=a) for a in
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
                 in_obj: mb_models.Medium) -> None:
        with get_db_session() as session:
            if isinstance(in_obj, mb_models.Medium):
                m: mb_models.Medium = session.merge(in_obj)

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
            + ( f" - {self.title}" if self.title else "")
        )


class Track(MusicBrainzObject):

    def __init__(self,
                 in_obj: TrackID | mb_models.Track | str) -> None:
        with get_db_session() as session:
            if isinstance(in_obj, mb_models.Track):
                tr: mb_models.Track = session.merge(in_obj)
            else:
                if isinstance(in_obj, str):
                    in_obj = TrackID(in_obj)
                stmt = sa.select(mb_models.Track).where(mb_models.Track.gid == str(in_obj))
                tr: mb_models.Track = session.scalar(stmt)

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
                 in_obj: WorkID | mb_models.Work | str) -> None:
        with get_db_session() as session:
            if isinstance(in_obj, mb_models.Work):
                w: mb_models.Work = session.merge(in_obj)
            else:
                if isinstance(in_obj, str):
                    in_obj = WorkID(in_obj)
                stmt = sa.select(mb_models.Work).where(mb_models.Work.gid == str(in_obj))
                w: mb_models.Work = session.scalar(stmt)

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
                sa.select(mb_models.Recording, mb_models.LinkAttribute)
                .select_from(
                    sa.join(
                        sa.join(mb_models.LinkRecordingWork, mb_models.Recording),
                        sa.join(mb_models.LinkAttribute, mb_models.Link),
                        isouter=True
                    )
                )
                .where(mb_models.LinkRecordingWork.entity1_id == str(self._db_id))
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

def get_artist(in_obj: ArtistID | str | mb_models.Artist) -> Artist:
    global _object_cache
    if isinstance(in_obj, mb_models.Artist):
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


def get_release_group(in_obj: ReleaseGroupID | str | mb_models.ReleaseGroup) -> ReleaseGroup:
    global _object_cache
    if isinstance(in_obj, mb_models.ReleaseGroup):
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


def get_release(in_obj: ReleaseID | str | mb_models.Release) -> Release:
    global _object_cache
    if isinstance(in_obj, mb_models.Release):
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


def get_recording(in_obj: RecordingID | str | mb_models.Recording) -> Recording:
    global _object_cache
    if isinstance(in_obj, mb_models.Recording):
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


def get_track(in_obj: TrackID | str | mb_models.Track) -> Track:
    global _object_cache
    if isinstance(in_obj, mb_models.Track):
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


def get_work(in_obj: WorkID | str | mb_models.Work) -> Work:
    global _object_cache
    if isinstance(in_obj, mb_models.Work):
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


def get_medium(mb_medium: mb_models.Medium = None) -> Medium:
    global _object_cache
    if mb_medium is not None:
        if mb_medium.id in _object_cache.keys():
            return _object_cache[mb_medium.id]
        else:
            a = Medium(mb_medium)
            _object_cache[mb_medium.id] = a
            return a
    else:
        raise MBApiError("No parameters given")
