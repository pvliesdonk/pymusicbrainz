import datetime
import logging
import re
from abc import ABC
from collections.abc import Generator
from functools import cached_property, cache
from typing import Optional

import mbdata.models
import rapidfuzz
import sqlalchemy as sa

from .constants import PRIMARY_TYPES, SECONDARY_TYPES, INT_COUNTRIES, FAVORITE_COUNTRIES
from .datatypes import ArtistID, ReleaseType, ReleaseID, ReleaseGroupID, RecordingID, TrackID, \
    WorkID, SecondaryTypeList, SearchType
from .db import get_db_session
from .exceptions import MBApiError, MBIDNotExistsError, NotFoundError

_logger = logging.getLogger(__name__)


class MusicBrainzObject(ABC):
    """Abstract object representing any of the primary Musicbrainz entities"""
    pass


class Artist(MusicBrainzObject):
    """Class representing an artist"""

    def __init__(self,
                 in_obj: ArtistID | mbdata.models.Artist | str) -> None:
        """Create Artist object. Use get_artist() instead

        :param in_obj: Musicbrainz ArtistID (optionally as string) or mbdata.models.Artist object
        """
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
    def country(self) -> str|None:
        from pymusicbrainz.util import area_to_country
        with get_db_session() as session:
            artist: mbdata.models.Artist = session.get(mbdata.models.Artist, self._db_id)
            result = None
            area: mbdata.models.Area = artist.area

            return area_to_country(area)


#select *
# from musicbrainz.area_containment as c
# left join musicbrainz.area as d on d.id = c.descendant
# left join musicbrainz.area as p on p.id = c.parent
# where c.descendant = 5155
# and p.type = 1

    def url(self) -> str:
        return f"https://musicbrainz.org/artist/{self.id}"

    @cache
    def _release_group_query(self,
                             primary_type: ReleaseType,
                             secondary_types: SecondaryTypeList,
                             credited: bool,
                             contributing: bool) -> sa.Select:
        """Create SQL query to get all release groups for this artist

        :param primary_type: only get release groups with this primary type
        :param secondary_types:  only get release groups with this secondary type.
        :param credited: Include release groups credited to this artist
        :param contributing: Include release groups where this artist contributes but is not credited
        :return:
        """

        # base: all  release groups for artist
        stmt = sa.select(mbdata.models.ReleaseGroup). \
            distinct(). \
            join(mbdata.models.ArtistReleaseGroup). \
            where(mbdata.models.ArtistReleaseGroup.artist.has(id=self._db_id)). \
            where(~mbdata.models.ArtistReleaseGroup.unofficial)

        if primary_type is ReleaseType.NONE:
            return stmt.where(sa.false())

        if ReleaseType.NONE in secondary_types:
            secondary_types = [ReleaseType.NONE]

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
        if primary_type is not ReleaseType.ALL:
            stmt = stmt.where(mbdata.models.ArtistReleaseGroup.primary_type == PRIMARY_TYPES[primary_type])

        if ReleaseType.NONE in secondary_types:
            stmt = stmt.where(mbdata.models.ArtistReleaseGroup.secondary_types.is_(None))
        elif ReleaseType.ALL not in secondary_types:
            if len(secondary_types) > 0:
                types = [SECONDARY_TYPES[t] for t in secondary_types]
                where_clause = mbdata.models.ArtistReleaseGroup.secondary_types.contains(types)
                stmt = stmt.where(where_clause)

        return stmt

    @cache
    def _get_release_group_db_items(self,
                                    primary_type: ReleaseType,
                                    secondary_types: SecondaryTypeList,
                                    credited: bool,
                                    contributing: bool) -> list[mbdata.models.ReleaseGroup]:
        """Fetch release groups for this artist from the database

        :param primary_type: only get release groups with this primary type
        :param secondary_types:  only get release groups with this secondary type.
        :param credited: Include release groups credited to this artist
        :param contributing: Include release groups where this artist contributes but is not credited
        :return:
        """

        s = f"Fetching"
        if primary_type is not None:
            s = s + f" {primary_type}s"
        else:
            s = s + " release groups"
        s = s + f" {'credited to' if credited else ''}{'/' if credited and contributing else ''}{'contributed to by' if contributing else ''}"
        s = s + f" artist {self.name} [{self.id}]"
        if secondary_types == [primary_type]:
            s = s + f" with no secondary types"
        else:
            if len(secondary_types) > 0:
                s = s + f" with secondary types {', '.join(secondary_types)}"

        _logger.debug(s)

        with get_db_session() as session:
            stmt = self._release_group_query(primary_type=primary_type, secondary_types=secondary_types,
                                             credited=credited, contributing=contributing)
            result: list[mbdata.models.ReleaseGroup] = session.scalars(stmt).all()

        return result

    @cache
    def get_release_groups(self,
                           primary_type: ReleaseType,
                           secondary_types: SecondaryTypeList,
                           credited: bool,
                           contributing: bool) -> list["ReleaseGroup"]:
        """Get all release groups for this artist

        :param primary_type: only get release groups with this primary type
        :param secondary_types:  only get release groups with this secondary type. When equal to primary_type, only return release groups with no secondary type
        :param credited: Include release groups credited to this artist
        :param contributing: Include release groups where this artist contributes but is not credited
        :return:
        """

        from .object_cache import get_release_group
        return [get_release_group(d) for d in
                self._get_release_group_db_items(primary_type, secondary_types, credited, contributing)]

    @cache
    def get_release_group_ids(self,
                              primary_type: ReleaseType,
                              secondary_types: SecondaryTypeList,
                              credited: bool,
                              contributing: bool) -> list["ReleaseGroupID"]:
        return [ReleaseGroupID(str(d.gid)) for d in
                self._get_release_group_db_items(primary_type, secondary_types, credited, contributing)]

    @cached_property
    def release_groups(self) -> list["ReleaseGroup"]:
        return self.get_release_groups(primary_type=ReleaseType.ALL,
                                       secondary_types=SecondaryTypeList([ReleaseType.ALL]), credited=True,
                                       contributing=False)

    @cached_property
    def release_group_ids(self) -> list["ReleaseGroupID"]:
        return self.get_release_group_ids(primary_type=ReleaseType.ALL,
                                          secondary_types=SecondaryTypeList([ReleaseType.ALL]), credited=True,
                                          contributing=False)

    @cached_property
    def albums(self) -> list["ReleaseGroup"]:
        return self.get_release_groups(primary_type=ReleaseType.ALBUM,
                                       secondary_types=SecondaryTypeList([ReleaseType.ALL]), credited=True,
                                       contributing=False)

    @cached_property
    def album_ids(self) -> list["ReleaseGroupID"]:
        return self.get_release_group_ids(primary_type=ReleaseType.ALBUM,
                                          secondary_types=SecondaryTypeList([ReleaseType.ALL]), credited=True,
                                          contributing=False)

    @cached_property
    def singles(self) -> list["ReleaseGroup"]:
        return self.get_release_groups(primary_type=ReleaseType.SINGLE,
                                       secondary_types=SecondaryTypeList([ReleaseType.ALL]), credited=True,
                                       contributing=False)

    @cached_property
    def single_ids(self) -> list["ReleaseGroupID"]:
        return self.get_release_group_ids(primary_type=ReleaseType.SINGLE,
                                          secondary_types=SecondaryTypeList([ReleaseType.ALL]), credited=True,
                                          contributing=False)

    @cached_property
    def eps(self) -> list["ReleaseGroup"]:
        return self.get_release_groups(primary_type=ReleaseType.EP,
                                       secondary_types=SecondaryTypeList([ReleaseType.ALL]), credited=True,
                                       contributing=False)

    @cached_property
    def ep_ids(self) -> list["ReleaseGroupID"]:
        return self.get_release_group_ids(primary_type=ReleaseType.EP,
                                          secondary_types=SecondaryTypeList([ReleaseType.ALL]), credited=True,
                                          contributing=False)

    @cached_property
    def studio_albums(self) -> list["ReleaseGroup"]:
        return self.get_release_groups(primary_type=ReleaseType.ALBUM,
                                       secondary_types=SecondaryTypeList([ReleaseType.NONE]), credited=True,
                                       contributing=False)

    @cached_property
    def studio_album_ids(self) -> list["ReleaseGroupID"]:
        return self.get_release_groups(primary_type=ReleaseType.ALBUM,
                                       secondary_types=SecondaryTypeList([ReleaseType.NONE]), credited=True,
                                       contributing=False)

    @cached_property
    def soundtracks(self) -> list["ReleaseGroup"]:
        return self.get_release_groups(primary_type=ReleaseType.ALBUM,
                                       secondary_types=SecondaryTypeList([ReleaseType.SOUNDTRACK]),
                                       credited=True, contributing=True)

    @cached_property
    def soundtrack_ids(self) -> list["ReleaseGroupID"]:
        return self.get_release_group_ids(primary_type=ReleaseType.ALBUM,
                                          secondary_types=SecondaryTypeList([ReleaseType.SOUNDTRACK]),
                                          credited=True, contributing=True)

    def is_sane(self, artist_query: str, cut_off=70) -> bool:
        from .util import split_artist, flatten_title

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
            return self in item.artists
        if isinstance(item, ReleaseGroup):
            return self in item.artists
        if isinstance(item, Recording):
            return self in item.artists
        if isinstance(item, Medium):
            return self in item.release.artists
        if isinstance(item, Track):
            return self in item.artists
        if isinstance(item, Work):
            raise NotImplementedError

    def __hash__(self):
        return hash(self.id)


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

    def __repr__(self):
        s1 = (f" [{self.countries[0]}]" if len(self.countries) == 1 else
              (f" [{self.countries[0]}+{len(self.countries)}]" if len(self.countries) > 1 else "")
              )
        s2 = (
            f" {self.first_release_date}" if self.first_release_date is not None else ""
        )
        return f"Release:  {self.artist_credit_phrase}: {self.title}{s2}{s1} [{self.id}]"

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
                    raise MBIDNotExistsError(f"Recording with id {in_obj} does not exist")

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
    def performance_of(self) -> list["Work"]:
        from .object_cache import get_work
        with get_db_session() as session:
            stmt = sa.select(mbdata.models.LinkRecordingWork). \
                where(mbdata.models.LinkRecordingWork.entity0_id == str(self._db_id))
            res: mbdata.models.LinkRecordingWork = session.scalars(stmt).all()
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

                [types.append(att.attribute_type.name) for att in res2 if att.attribute_type.name not in types]

            self.performance_type = types

        return ws

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
        _logger.debug(f"Computing siblings of {self.__repr__()}")
        works = self.performance_of
        for work in works:
            if len(self.performance_type) == 0:
                for r in work.performances['no-attr']:
                    if r not in result and r.artists == self.artists:
                        result.append(r)
            elif len(self.performance_type) == 1:
                _logger.debug(
                    f"Recording of type {self.performance_type[0]}; returning matching siblings of {self.artist_credit_phrase} - {self.title}")
                for r in work.performances[self.performance_type[0]]:
                    if r not in result and r.artists == self.artists:
                        result.append(r)
            else:
                _logger.debug(
                    f"Recording of types {'/'.join(self.performance_type)}; returning matching siblings of {self.artist_credit_phrase} - {self.title}")
                options = work.performances[self.performance_type[0]]
                for i in range(1, len(self.performance_type)):
                    options = [rec for rec in options if rec in work.performances[self.performance_type[i]]]

                for r in options:
                    if r not in result and r.artists == self.artists:
                        result.append(r)
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
            _logger.error(f"{self} is not a sane candidate for artist {artist_query}")
        elif title_ratio < cut_off:
            _logger.error(f"{self} is not a sane candidate for title {title_query}")
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
            self.format = m.format.name if m.format is not None else None

    @cached_property
    def release(self) -> Release:
        from .object_cache import get_release
        return get_release(self._release_id)

    @cached_property
    def tracks(self) -> list["Track"]:
        from .object_cache import get_track
        return [get_track(t) for t in self._track_ids]

    def __repr__(self):
        return (
                f"Medium: {self.release.artist_credit_phrase} - {self.release.title}"
                + (f" - {self.title}" if self.title else "")
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

    def __repr__(self):
        return f"Track {self.position}/{self.medium.track_count} of {self.release.artist_credit_phrase} - {self.release.title} / {self.recording.artist_credit_phrase} - {self.recording.title}"

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

            self.id: WorkID = WorkID(str(w.gid))
            self._db_id: int = w.id
            self.title: str = w.name
            self.disambiguation: str = w.comment
            self.type: str = w.type.name if w.type is not None else None

    @cached_property
    def performances(self) -> dict[str, list[Recording]]:
        results = {'all': [], 'no-attr': []}
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
        if release is None or track is None:
            self.release, self.track = find_track_release_for_release_group_recording(release_group, recording)
        else:
            self.release = release
            self.track = track

    def __repr__(self):
        return self.track.__repr__()

    def __lt__(self, other):
        if isinstance(other, MusicbrainzSingleResult):
            return self.track < other.track

    def __eq__(self, other):
        if isinstance(other, MusicbrainzSingleResult):
            return self.release_group == other.release_group and self.recording == other.recording

class MusicbrainzListResult(list[MusicbrainzSingleResult]):

    pass


class MusicbrainzSearchResult:

    def __init__(self):
        self._dict : dict[SearchType, MusicbrainzListResult] = {}

    def add_result(self, search_type: SearchType, result: MusicbrainzListResult) -> None:
        self._dict[search_type] = result

    def get_result(self, search_type: SearchType) -> Optional[MusicbrainzSingleResult]:
        if search_type in self._dict.keys():
            self._dict[search_type].sort()
            return self._dict[search_type][0]
        return None

    def is_empty(self) -> bool:
        return len(self._dict) == 0

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

    def get_best_result(self) -> Optional[MusicbrainzSingleResult]:

        choice = None
        if self.canonical is not None:
            _logger.debug("Found canonical result")
            choice = SearchType.CANONICAL

        if self.studio_album is not None:
            if self.studio_album != self.canonical:
                _logger.debug("Switching to studio album result")
                choice = SearchType.STUDIO_ALBUM
            # else keep canonical
            if self.soundtrack is not None:
                if self.soundtrack < self.studio_album:
                    _logger.debug("Found soundrack older than studio album")
                    choice = SearchType.SOUNDTRACK
        elif self.ep is not None:
            if self.ep != self.canonical:
                _logger.debug("Switching to EP result")
                choice = SearchType.EP
            if self.soundtrack is not None:
                if self.soundtrack < self.ep:
                    _logger.debug("Found soundrack older than ep")
                    choice = SearchType.SOUNDTRACK

        elif self.soundtrack is not None:
            if self.soundtrack != self.canonical:
                _logger.debug("Switching to soundtrack result")
                choice = SearchType.SOUNDTRACK
            if self.single is not None:
                if self.single < self.soundtrack:
                    _logger.debug("Switching to single older than soundtrack")
                    choice = SearchType.SINGLE
        else:
            raise NotFoundError()

        return self.get_result(choice)

    def __repr__(self):
        return "(Search result) best result:" + self.get_best_result().track.__repr__()


def find_track_for_release_recording(release: Release, recording: Recording) -> Track:
    potential_results = []
    for track in release.tracks:
        if track.recording == recording:
            potential_results.append(track)
    return min(potential_results)


def find_track_release_for_release_group_recording(rg: ReleaseGroup, recording: Recording) -> tuple[Release, Track]:
    potential_results = []
    for r in rg.releases:
        for track in r.tracks:
            if track.recording == recording:
                potential_results.append((r, track))
    # do some sorting/selection
    return min(potential_results)
