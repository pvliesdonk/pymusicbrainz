import datetime
import logging
import re
from abc import ABC
from collections.abc import Generator
from functools import cached_property, cache
from typing import Optional, Any

import mbdata.models
import rapidfuzz
import sqlalchemy as sa

from .constants import INT_COUNTRIES, FAVORITE_COUNTRIES
from .datatypes import ReleaseType, SearchType, PerformanceWorkAttributes
from . import ArtistID, ReleaseGroupID, ReleaseID, RecordingID, TrackID, WorkID
from .db import get_db_session
from .exceptions import MBIDNotExistsError, NotFoundError, IllegaleRecordingReleaseGroupCombination


_logger = logging.getLogger(__name__)

def escape(s: Any) -> str:
    return re.sub(r'\'', '\\\'', str(s))




class ReleaseGroup(MusicBrainzObject):

    def __init__(self,
                 in_obj: ReleaseGroupID | mbdata.models.ReleaseGroup | str) -> None:

        from .object_cache import get_artist

        with get_db_session() as session:
            if isinstance(in_obj, mbdata.models.ReleaseGroup):

                rg: mbdata.models.ReleaseGroup = session.merge(in_obj)
            else:
                if isinstance(in_obj, str):
                    in_obj = ReleaseGroupID(in_obj)
                stmt = sa.select(mbdata.models.ReleaseGroup).where(mbdata.models.ReleaseGroup.gid == str(in_obj))
                rg: mbdata.models.ReleaseGroup = session.scalar(stmt)

                if rg is None:
                    raise MBIDNotExistsError(f"No Release Group with ID '{str(in_obj)}'")

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
        from .util import parse_partial_date
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
    def _releases_db_items(self) -> list["mbdata.models.Release"]:
        with get_db_session() as session:
            stmt = sa.select(mbdata.models.Release).where(mbdata.models.Release.release_group_id == self._db_id)
            releases: list[mbdata.models.Release] = session.scalars(stmt).all()

            return releases

    @cached_property
    def releases(self) -> list["Release"]:
        from .object_cache import get_release
        return sorted([get_release(release) for release in self._releases_db_items])

    @cached_property
    def release_ids(self) -> list["ReleaseID"]:
        return [ReleaseID(str(release.gid)) for release in self._releases_db_items]

    @cached_property
    def _recordings_db_items(self) -> list["mbdata.models.Recording"]:
        with get_db_session() as session:
            stmt = (
                sa.select(mbdata.models.Recording)
                .join(mbdata.models.Track)
                .join(mbdata.models.Medium)
                .join(mbdata.models.Release)
                .where(mbdata.models.Release.release_group.has(id=self._db_id))
            )
            recordings: list[mbdata.models.Recording] = session.scalars(stmt).all()

            return recordings

    @cached_property
    def recordings(self) -> list["Recording"]:
        from .object_cache import get_recording
        return [get_recording(recording) for recording in self._recordings_db_items]

    @cached_property
    def recording_ids(self) -> list["RecordingID"]:
        return [RecordingID(str(recording.gid)) for recording in self._recordings_db_items]

    def is_sane(self, artist_query: str, title_query: str, cut_off=70) -> bool:
        from .util import flatten_title

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


    def __str__(self):
        s1 = f" [{self.primary_type}]" if self.primary_type is not None else ""
        s2 = (
            f" {self.first_release_date}" if self.first_release_date is not None else ""
        )
        return f"'{self.artist_credit_phrase}' - '{self.title}'{s1}{s2} [{self.id}]"

    def __rich__(self):
        s1 = f" [{self.primary_type}]" if self.primary_type is not None else ""
        s2 = (
            f" {self.first_release_date}" if self.first_release_date is not None else ""
        )
        return f"'{escape(self.artist_credit_phrase)}' - '{escape(self.title)}'{s1}{s2} \[[link={self.url}]{self.id}[/link]\]"

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
            return item in self.artists
        if isinstance(item, Release):
            return item.release_group == self
        if isinstance(item, Recording):
            return item.id in self.recording_ids
        if isinstance(item, Medium):
            return item.release.release_group == self
        if isinstance(item, Track):
            return item.release.release_group == self
        if isinstance(item, Work):
            raise NotImplementedError

    def __hash__(self):
        return hash(self.id)


class Release(MusicBrainzObject):

    def __init__(self,
                 in_obj: ReleaseID | mbdata.models.Release | str) -> None:
        from .object_cache import get_artist
        from .util import parse_partial_date
        from pymusicbrainz.util import area_to_country
        with get_db_session() as session:
            if isinstance(in_obj, mbdata.models.Release):
                rel: mbdata.models.Release = session.merge(in_obj)
            else:
                if isinstance(in_obj, str):
                    in_obj = ReleaseID(in_obj)
                stmt = sa.select(mbdata.models.Release).where(mbdata.models.Release.gid == str(in_obj))
                rel: mbdata.models.Release = session.scalar(stmt)

                if rel is None:
                    raise MBIDNotExistsError(f"No Release with ID '{str(in_obj)}'")

            self.id: ReleaseID = ReleaseID(str(rel.gid))
            self._db_id: int = rel.id
            self.artists = [get_artist(ArtistID(str(a.artist.gid))) for a in rel.artist_credit.artists]
            self.title: str = rel.name
            self._release_group_id: ReleaseGroupID = ReleaseGroupID(str(rel.release_group.gid))
            self.artist_credit_phrase: str = rel.artist_credit.name
            self.disambiguation: str = rel.comment
            self.first_release_date: datetime.date = parse_partial_date(
                rel.first_release.date) if rel.first_release is not None else None
            self.countries: list[str] = [area_to_country(c.country.area) for c in rel.country_dates]

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
    def is_country_of_artist(self) -> bool:
        return any([a.country in self.countries for a in self.artists])

    @cached_property
    def is_international_release(self) -> bool:
        return any([c in self.countries for c in INT_COUNTRIES])

    @cached_property
    def is_favorite_country(self) -> bool:
        return any([c in self.countries for c in FAVORITE_COUNTRIES])

    @cached_property
    def release_group(self) -> ReleaseGroup:
        from .object_cache import get_release_group
        return get_release_group(self._release_group_id)

    @cached_property
    def mediums(self) -> list["Medium"]:
        from .object_cache import get_medium
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
    def _recordings_db_items(self) -> list["mbdata.models.Recording"]:
        with get_db_session() as session:
            stmt = (
                sa.select(mbdata.models.Recording)
                .join(mbdata.models.Track)
                .join(mbdata.models.Medium)
                .where(mbdata.models.Medium.release.has(id=self._db_id))
            )
            recordings: list[mbdata.models.Recording] = session.scalars(stmt).all()

            return recordings

    @cached_property
    def recordings(self) -> list["Recording"]:
        from .object_cache import get_recording
        return [get_recording(recording) for recording in self._recordings_db_items]

    @cached_property
    def recording_ids(self) -> list["RecordingID"]:
        return [RecordingID(str(recording.gid)) for recording in self._recordings_db_items]

    def is_sane(self, artist_query: str, title_query: str, cut_off=70) -> bool:
        from .util import flatten_title
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

    def __str__(self):
        s1 = (f" [{self.countries[0]}]" if len(self.countries) == 1 else
              (f" [{self.countries[0]}+{len(self.countries)}]" if len(self.countries) > 1 else "")
              )
        s2 = (
            f" {self.first_release_date}" if self.first_release_date is not None else ""
        )
        return f"'{self.artist_credit_phrase}' - '{self.title}'{s2}{s1} [{self.id}]"

    def __rich__(self):
        s1 = (f" [{self.countries[0]}]" if len(self.countries) == 1 else
              (f" [{self.countries[0]}+{len(self.countries)}]" if len(self.countries) > 1 else "")
              )
        s2 = (
            f" {self.first_release_date}" if self.first_release_date is not None else ""
        )
        return f"'{escape(self.artist_credit_phrase)}' - '{escape(self.title)}'{s2}{s1} \[[link={self.url}]{self.id}[/link]\]"

    def __eq__(self, other):
        if isinstance(other, Release):
            return self.id == other.id
        else:
            return False

    def __contains__(self, item):
        if isinstance(item, Artist):
            return item in self.artists
        if isinstance(item, ReleaseGroup):
            return self.release_group == item
        if isinstance(item, Recording):
            return item in self.recordings
        if isinstance(item, Medium):
            return item.release == item
        if isinstance(item, Track):
            return item.release == item
        if isinstance(item, Work):
            raise NotImplementedError

    def __lt__(self, other):
        if isinstance(other, Release):

            if self.first_release_date is not None:
                if other.first_release_date is not None:
                    if self.first_release_date != other.first_release_date:
                        return self.first_release_date < other.first_release_date
                    elif self.is_country_of_artist != other.is_country_of_artist:
                        return self.is_country_of_artist > other.is_country_of_artist
                    elif self.is_favorite_country != other.is_favorite_country:
                        return self.is_favorite_country > other.is_favorite_country
                    else:
                        #_logger.error("Multiple releases with same date and country:")
                        #_logger.error(self)
                        #_logger.error(other)
                        return True
                else:
                    return True
            else:
                return False

    def __hash__(self):
        return hash(self.id)


class Recording(MusicBrainzObject):

    def __init__(self,
                 in_obj: RecordingID | mbdata.models.Recording | str) -> None:
        from .object_cache import get_artist
        from .util import parse_partial_date
        with get_db_session() as session:
            if isinstance(in_obj, mbdata.models.Recording):
                rec: mbdata.models.Recording = session.merge(in_obj)
            else:
                if isinstance(in_obj, str):
                    in_obj = RecordingID(in_obj)
                stmt = sa.select(mbdata.models.Recording).where(mbdata.models.Recording.gid == str(in_obj))
                rec: mbdata.models.Recording = session.scalar(stmt)
                if rec is None:
                    raise MBIDNotExistsError(f"No recording with id '{in_obj}'")

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
    def performance_type(self) -> list[PerformanceWorkAttributes]:
        p = self.performance_of
        return self.performance_type

    @cached_property
    def performance_of(self) -> list["Work"]:
        from .object_cache import get_work
        with get_db_session() as session:
            stmt = sa.select(mbdata.models.LinkRecordingWork). \
                where(mbdata.models.LinkRecordingWork.entity0_id == str(self._db_id))
            res: list[mbdata.models.LinkRecordingWork] = session.scalars(stmt).all()
            if res is None or len(res) == 0:
                self.performance_type = []
                return []
            else:
                ws = [get_work(r.work) for r in res]

            types = []
            for r in res:
                stmt = sa.select(mbdata.models.LinkAttribute). \
                    where(mbdata.models.LinkAttribute.link == r.link)
                res2: list[mbdata.models.LinkAttribute] = session.scalars(stmt).all()

                [types.append(PerformanceWorkAttributes(att.attribute_type.name)) for att in res2 if PerformanceWorkAttributes(att.attribute_type.name) not in types]

            self.performance_type = types

        return ws

    @cached_property
    def is_acapella(self) -> bool:
        return PerformanceWorkAttributes.ACAPELLA in self.performance_type

    @cached_property
    def is_live(self) -> bool:
        return PerformanceWorkAttributes.LIVE in self.performance_type

    @cached_property
    def is_medley(self) -> bool:
        return PerformanceWorkAttributes.MEDLEY in self.performance_type

    @cached_property
    def is_partial(self) -> bool:
        return PerformanceWorkAttributes.PARTIAL in self.performance_type

    @cached_property
    def is_instrumental(self) -> bool:
        return PerformanceWorkAttributes.INSTRUMENTAL in self.performance_type

    @cached_property
    def is_cover(self) -> bool:
        return PerformanceWorkAttributes.COVER in self.performance_type

    @cached_property
    def is_karaoke(self) -> bool:
        return PerformanceWorkAttributes.KARAOKE in self.performance_type

    @cached_property
    def is_normal_performance(self) -> bool:
        return len(self.performance_type) == 0

    @cached_property
    def siblings(self) -> list["Recording"]:
        result = []
        _logger.debug(f"Computing siblings of {self}")
        works = self.performance_of
        for work in works:
            if len(self.performance_type) == 0:
                for r in work.performance_by_type([PerformanceWorkAttributes.NONE]):
                    if r not in result and r.artists == self.artists:
                        result.append(r)
            else:
                _logger.debug(
                    f"Recording of types {'/'.join(self.performance_type)}; returning matching siblings of {self.artist_credit_phrase} - {self.title}")

                result = [rec for rec in work.performance_by_type(self.performance_type) if rec.artists == self.artists]
        _logger.debug(f"Identified {len(result)} siblings")
        return result



    # @cached_property
    # def streams(self) -> list[str]:
    #     result = []
    #     with get_db_session() as session:
    #
    #         base_stmt = (
    #             sa.select(mbdata.models.URL, mbdata.models.Link, mbdata.models.LinkAttribute)
    #             .select_from(
    #                 sa.join(
    #                     sa.join(mbdata.models.URL, mbdata.models.LinkRecordingURL).join(mbdata.models.Recording),
    #                     sa.join(mbdata.models.Link, mbdata.models.LinkAttribute),
    #                     isouter=True
    #                 ))
    #         )
    #         stmt = base_stmt.where(mbdata.models.LinkRecordingURL.recording_id == str(self._db_id))
    #
    #         res: sa.ChunkedIteratorResult = session.execute(stmt)
    #
    #         if res.raw.rowcount == 0:
    #             _logger.debug(f"Also looking for streams of siblings")
    #
    #             siblings = [str(s.id) for s in self.siblings]
    #
    #             stmt = base_stmt.where(mbdata.models.Recording.gid.in_(siblings))
    #             res: list[mbdata.models.URL, mbdata.models.Link, mbdata.models.LinkAttribute] = session.execute(stmt)
    #
    #         for (url, link, la) in res:
    #             if la is not None:
    #                 if la.attribute_type_id == 582:  # video
    #                     continue
    #             if url.url not in result:
    #                 result.append(url.url)
    #
    #     return result
    #
    # @cached_property
    # def spotify_id(self) -> str | None:
    #     spotify_id_regex = r'open\.spotify\.com/\w+/([0-9A-Za-z]+)'
    #     for url in self.streams:
    #         match = re.search(spotify_id_regex, url)
    #         if match:
    #             id_ = match.group(1)
    #             if id_:
    #                 return id_
    #     return None

    def __str__(self):
        s_date = f" {self.first_release_date}" if self.first_release_date is not None else ""
        return f"'{self.artist_credit_phrase}' - '{self.title}'{s_date} [{self.id}] " + (
            "/".join(self.performance_type) if len(self.performance_type) > 0 else "")


    def __rich__(self):
        s_date = f" {self.first_release_date}" if self.first_release_date is not None else ""
        return f"'{escape(self.artist_credit_phrase)}' - '{escape(self.title)}'{s_date} \[[link={self.url}]{self.id}[/link]\] " + (
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

    def __contains__(self, item):
        if isinstance(item, Artist):
            return item in self.artists
        if isinstance(item, ReleaseGroup):
            return self in item.recordings
        if isinstance(item, Release):
            return self in item.recordings
        if isinstance(item, Medium):
            return any([self == t.recording for t in item.tracks])
        if isinstance(item, Track):
            return item.recording == self
        if isinstance(item, Work):
            return self in item.performances['all']

    def is_sane(self, artist_query: str, title_query: str, cut_off=70) -> bool:
        from .util import flatten_title
        artist_sane = any([artist.is_sane(artist_query) for artist in self.artists])

        title_ratio = rapidfuzz.process.extractOne(
            flatten_title(recording_name=title_query),
            [flatten_title(recording_name=self.title)] + [flatten_title(recording_name=a) for a in
                                                          self.aliases],
            processor=rapidfuzz.utils.default_process
        )[1]

        if not artist_sane:
            _logger.warning(f"{self} is not a sane candidate for artist {artist_query}")
        elif title_ratio < cut_off:
            _logger.warning(f"{self} is not a sane candidate for title {title_query}")
        else:
            return True

    @cached_property
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
            self.format = m.format.name if m.format is not None else None

    @cached_property
    def release(self) -> Release:
        from .object_cache import get_release
        return get_release(self._release_id)

    @cached_property
    def tracks(self) -> list["Track"]:
        from .object_cache import get_track
        return [get_track(t) for t in self._track_ids]

    def __str__(self):
        return (
                f"'{self.release.artist_credit_phrase}' - '{self.release.title}'"
                + (f" - '{self.title}'" if self.title else "")
        )

    def __rich__(self):
        return (
                f"'{escape(self.release.artist_credit_phrase)}' - '{escape(self.release.title)}'"
                + (f" - '{escape(self.title)}'" if self.title else "")
        )

    def __contains__(self, item):
        if isinstance(item, Artist):
            return any([item in t.artists for t in self.tracks])
        if isinstance(item, ReleaseGroup):
            return self.release.release_group == item
        if isinstance(item, Release):
            return self.release == item
        if isinstance(item, Recording):
            return any([item == t.recording for t in self.tracks])
        if isinstance(item, Track):
            return item in self.tracks
        if isinstance(item, Work):
            raise NotImplementedError


class Track(MusicBrainzObject):

    def __init__(self,
                 in_obj: TrackID | mbdata.models.Track | str) -> None:
        from .object_cache import get_artist, get_medium
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
        from .object_cache import get_recording
        return get_recording(self._recording_id)

    @cached_property
    def release(self) -> Release:
        return self.medium.release

    def __lt__(self, other):
        if isinstance(other, Track):
            if self.release == other.release:
                return self.position < other.position
            else:
                return self.release < other.release

    def __str__(self):
        return f"{self.position}/{self.medium.track_count} of '{self.release.artist_credit_phrase}' - '{self.release.title}': '{self.recording.artist_credit_phrase}' - '{self.recording.title}'"

    def __rich__(self):
        return f"{self.position}/{self.medium.track_count} of '{escape(self.release.artist_credit_phrase)}' - '{escape(self.release.title)}': '{escape(self.recording.artist_credit_phrase)}' - '{escape(self.recording.title)}'"

    def __contains__(self, item):
        if isinstance(item, Artist):
            return item in self.recording.artists
        if isinstance(item, ReleaseGroup):
            return self.release.release_group == item
        if isinstance(item, Release):
            return self.release == item
        if isinstance(item, Medium):
            return self.medium == item
        if isinstance(item, Recording):
            return self.recording == item
        if isinstance(item, Work):
            return self.recording in item.performances['all']


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

                if w is None:
                    raise MBIDNotExistsError(f"No Work with ID '{str(in_obj)}'")

            self.id: WorkID = WorkID(str(w.gid))
            self._db_id: int = w.id
            self.title: str = w.name
            self.disambiguation: str = w.comment
            self.type: str = w.type.name if w.type is not None else None

    @cached_property
    def performances(self) -> dict[PerformanceWorkAttributes, list[Recording]]:
        results = {PerformanceWorkAttributes.ALL: [], PerformanceWorkAttributes.NONE: []}
        from .object_cache import get_recording
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
                if rec not in results[PerformanceWorkAttributes.ALL]:
                    results[PerformanceWorkAttributes.ALL].append(rec)

                if la is None:
                    results[PerformanceWorkAttributes.NONE].append(rec)
                else:
                    att = PerformanceWorkAttributes(la.attribute_type.name)
                    if att in results.keys():
                        results[att].append(rec)
                    else:
                        results[att] = [rec]

        return results

    def performance_by_type(self, types: list[PerformanceWorkAttributes]) -> list[Recording]:
        results = None
        for t in types:
            if t in self.performances.keys():
                if results is None:
                    results = self.performances[t]
                else:
                    results = [r for r in results if r in self.performances[t]]
                    results = list(set(results))
        if results is None:
            return []
        return results

    def __str__(self):
        return f"{self.title}  [{self.id}]"

    def __rich__(self):
        return f"{escape(self.title)}  \[[link={self.url}]{self.id}[/link]\]"

    def __eq__(self, other):
        if isinstance(other, Work):
            return self.id == other.id
        else:
            return False

    @cached_property
    def url(self) -> str:
        return f"https://musicbrainz.org/work/{self.id}"

    def __hash__(self):
        return hash(self.id)

    def __contains__(self, item):
        if isinstance(item, Artist):
            raise NotImplementedError
        if isinstance(item, ReleaseGroup):
            raise NotImplementedError
        if isinstance(item, Release):
            raise NotImplementedError
        if isinstance(item, Medium):
            raise NotImplementedError
        if isinstance(item, Track):
            return item.recording in self.performances['all']
        if isinstance(item, Recording):
            return item in self.performances['all']


class MusicbrainzSingleResult:

    def __init__(self,
                 release_group: ReleaseGroup,
                 recording: Recording,
                 release: Optional[Release] = None,
                 track: Optional[Track] = None):
        self.release_group = release_group
        self.recording = recording
        if release is None:
            try:
                self.release, self.track = find_track_release_for_release_group_recording(self.release_group, self.recording)
            except IllegaleRecordingReleaseGroupCombination as ex:
                raise ex
        elif track is None:
            try:
                self.release = release
                self.track = find_track_for_release_recording(self.release, self.recording)
            except IllegaleRecordingReleaseGroupCombination as ex:
                raise ex
        else:
            self.release = release
            self.track = track

        if self.release.release_group.id != self.release_group.id:
            _logger.warning(f"Git a strange combination of {self.release} with {self.release_group}. Fixing.")
            self.release_group = self.release.release_group

    def __repr__(self):
        return self.track.__repr__()

    def __rich__(self):
        from rich.markup import escape
        return escape(self.__repr__())

    def __lt__(self, other):
        if isinstance(other, MusicbrainzSingleResult):
            return self.track < other.track

    def __eq__(self, other):
        if isinstance(other, MusicbrainzSingleResult):
            return self.release_group == other.release_group and self.recording == other.recording

class MusicbrainzListResult(list[MusicbrainzSingleResult]):

    pass


class MusicbrainzSearchResult:

    def __init__(self, live: bool = False):
        self._dict : dict[SearchType, MusicbrainzListResult] = {}
        self.live = live

    def add_result(self, search_type: SearchType, result: MusicbrainzListResult) -> None:
        self._dict[search_type] = result

    def get_result(self, search_type: SearchType) -> Optional[MusicbrainzSingleResult]:
        if search_type in self._dict.keys() and len(self._dict[search_type])>0:
            self._dict[search_type].sort()
            return self._dict[search_type][0]
        return None

    def is_empty(self) -> bool:
        if len(self._dict) == 0:
            return True
        if all([len(x) == 0 for x in self._dict.items()]):
            return True
        return False

    @property
    def canonical(self) -> Optional[MusicbrainzSingleResult]:
        return self.get_result(SearchType.CANONICAL)


    @property
    def studio_album(self) -> Optional[MusicbrainzSingleResult]:
        return self.get_result(SearchType.STUDIO_ALBUM)

    @property
    def all(self) -> Optional[MusicbrainzSingleResult]:
        return self.get_result(SearchType.ALL)

    @property
    def single(self) -> Optional[MusicbrainzSingleResult]:
        return self.get_result(SearchType.SINGLE)

    @property
    def ep(self) -> Optional[MusicbrainzSingleResult]:
        return self.get_result(SearchType.EP)

    @property
    def soundtrack(self) -> Optional[MusicbrainzSingleResult]:
        return self.get_result(SearchType.SOUNDTRACK)



    def iterate_results(self) -> Generator[SearchType, MusicbrainzSingleResult]:
        for search_type in SearchType:
            r = self.get_result(search_type)
            if r is not None:
                yield search_type, r

    @cache
    def get_best_result(self) -> Optional[MusicbrainzSingleResult]:

        if self.is_empty():								# something exists
            raise NotFoundError("Result is empty")

        choice = None						
        if self.canonical is not None:
            choice = SearchType.CANONICAL

        if self.studio_album is not None:						# there may be no canonical
            if self.studio_album != self.canonical:
                choice = SearchType.STUDIO_ALBUM
            # else keep canonical
            if self.soundtrack is not None:
                if self.soundtrack < self.studio_album:
                    _logger.debug("Found soundtrack older than studio album")
                    choice = SearchType.SOUNDTRACK
        elif self.ep is not None:							# there is no album
            if self.ep != self.canonical:
                choice = SearchType.EP
            if self.soundtrack is not None:
                if self.soundtrack < self.ep:
                    _logger.debug("Found soundtrack older than ep")
                    choice = SearchType.SOUNDTRACK

        elif self.soundtrack is not None:						# there is no ep
            if self.soundtrack != self.canonical:
                choice = SearchType.SOUNDTRACK
            if self.single is not None:
                if self.single < self.soundtrack:
                    _logger.debug("Found single older than soundtrack")
                    choice = SearchType.SINGLE
        
        elif choice is None and self.single is not None:
            _logger.debug("No other release found, but Single is available")
            choice = SearchType.SINGLE

        elif choice is None and self.all is not None:
            _logger.debug("No other release found, but found something outside my predefined categories")
            choice = SearchType.ALL
        
        # should never get here
        if choice is None:
            raise NotFoundError("Was not able to determine a best result for non-empy result set")
        else:
            _logger.debug(f"Best Musicbrainz result is of type {str(choice)}")

        return self.get_result(choice)

    def __repr__(self):
        return "(Search result) best result:" + self.get_best_result().track.__repr__()

    def __rich__(self):
        from rich.markup import escape
        return escape(self.__repr__())

def find_track_for_release_recording(release: Release, recording: Recording) -> Track:
    potential_results = []
    for track in release.tracks:
        if track.recording == recording:
            potential_results.append(track)
    if len(potential_results) == 0:
        raise IllegaleRecordingReleaseGroupCombination(f"Release {release} does not contain Recording {recording}")
    return min(potential_results)


def find_track_release_for_release_group_recording(rg: ReleaseGroup, recording: Recording) -> tuple[Release, Track]:
    potential_results = []
    for r in rg.releases:
        for track in r.tracks:
            if track.recording == recording:
                potential_results.append((r, track))
    # do some sorting/selection
    if len(potential_results) == 0:
        raise IllegaleRecordingReleaseGroupCombination(f"Release Group {rg} does not contain Recording {recording}")
    return min(potential_results)

