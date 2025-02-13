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

from .constants import PRIMARY_TYPES, SECONDARY_TYPES, INT_COUNTRIES, FAVORITE_COUNTRIES, VA_ARTIST_ID
from .datatypes import ArtistID, ReleaseType, ReleaseID, ReleaseGroupID, RecordingID, TrackID, \
    WorkID, SecondaryTypeList, SearchType, PerformanceWorkAttributes
from .db import get_db_session
from .exceptions import MBApiError, MBIDNotExistsError, NotFoundError, IllegaleRecordingReleaseGroupCombination

_logger = logging.getLogger(__name__)


def _abs_for_none(a: Optional[int]) -> int:
    if a is None:
        return 100
    else:
        return abs(a)


def escape(s: Any) -> str:
    return re.sub(r'\'', '\\\'', str(s))


class MusicBrainzObject(ABC):
    """Abstract object representing any of the primary Musicbrainz entities"""

    def __repr__(self):
        return f"{self.__class__.__name__}({self.__str__()})"


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
                if a is None:
                    raise MBIDNotExistsError(f"No Artist with ID '{str(in_obj)}'")

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

    def is_alias(self, artist) -> bool:
        from pymusicbrainz.util import flatten_title
        y = flatten_title(artist_name=artist)
        return any([y == flatten_title(artist_name=x) for x in self.aliases])

    @cached_property
    def country(self) -> str | None:
        from .util import area_to_country
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

    @cached_property
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
                                    contributing: bool
                                    ) -> list[mbdata.models.ReleaseGroup]:
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
            _logger.debug(f"Found {len(result)} release groups matching criteria")

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
        return self.get_release_group_ids(primary_type=ReleaseType.ALBUM,
                                          secondary_types=SecondaryTypeList([ReleaseType.NONE]), credited=True,
                                          contributing=False)

    @cached_property
    def compilations(self) -> list["ReleaseGroup"]:
        return self.get_release_groups(primary_type=ReleaseType.ALBUM,
                                       secondary_types=SecondaryTypeList([ReleaseType.COMPILATION]), credited=True,
                                       contributing=False)

    @cached_property
    def compilation_ids(self) -> list["ReleaseGroupID"]:
        return self.get_release_group_ids(primary_type=ReleaseType.ALBUM,
                                          secondary_types=SecondaryTypeList([ReleaseType.COMPILATION]), credited=True,
                                          contributing=False)

    @cached_property
    def live_albums(self) -> list["ReleaseGroup"]:
        return self.get_release_groups(primary_type=ReleaseType.ALBUM,
                                       secondary_types=SecondaryTypeList([ReleaseType.LIVE]), credited=True,
                                       contributing=False)

    @cached_property
    def live_album_ids(self) -> list["ReleaseGroupID"]:
        return self.get_release_group_ids(primary_type=ReleaseType.ALBUM,
                                          secondary_types=SecondaryTypeList([ReleaseType.LIVE]), credited=True,
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

    def is_sane(self, artist_query: "str | Artist", cut_off=70) -> bool:
        if isinstance(artist_query, Artist):
            return self.__eq__(artist_query)

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

    @cached_property
    def external_urls(self) -> dict[str, str]:
        out = {}
        with get_db_session() as session:
            stmt = (
                sa.select(mbdata.models.LinkArtistURL)
                .options(
                    sa.orm.selectinload(mbdata.models.LinkArtistURL.url))
                .options(
                    sa.orm.selectinload(mbdata.models.LinkArtistURL.link)
                    .selectinload(mbdata.models.Link.link_type)
                )

                .where(mbdata.models.LinkArtistURL.artist_id == self._db_id)
            )
            lrus = session.scalars(stmt).all()
            lru: mbdata.models.LinkReleaseGroupURL
            for lru in lrus:
                url = lru.url.url
                urltype = str(lru.link.link_type.link_phrase)
                if urltype not in out.keys():
                    out[urltype] = [url]
                else:
                    out[urltype].append(url)

        return out

    @cached_property
    def discogs_ids(self) -> list[tuple[str, int]]:
        if "Discogs" in self.external_urls.keys():
            urls = [u.rsplit('/')[-1] for u in self.external_urls["Discogs"]]
            ids = [('artist', int(u)) for u in urls]
            return ids
        return []

    @cached_property
    def spotify_link(self) -> list[str]:
        urls = []
        if "stream {video} for free" in self.external_urls.keys():
            urls = [u for u in self.external_urls["stream {video} for free"] if "open.spotify.com" in u]
        return urls

    def __str__(self):
        if self.disambiguation is not None:
            return f"{self.name} [{self.id}] ({self.disambiguation})"
        else:
            return f"{self.name} [{self.id}]"

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

    def is_years_older_than(self, other: "ReleaseGroup|int") -> Optional[int]:
        if isinstance(other, ReleaseGroup):
            if self.first_release_date is None or other.first_release_date is None:
                return None
            delta = other.first_release_date - self.first_release_date
            years = (delta.days + 1 if delta.days < 0 else delta.days) // 365
            # _logger.debug(f"{self} is {years} years older than {other}")
            return years + 1 if years < 0 else years
        elif isinstance(other, int):
            if self.first_release_date is None:
                return None
            return other - self.first_release_date.year
        else:
            raise NotImplementedError

    @cached_property
    def is_live_album(self) -> bool:
        return self.primary_type == ReleaseType.ALBUM and len(self.types) == 2 and ReleaseType.LIVE in self.types

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
    def min_track_count(self) -> int:
        return min([r.track_count for r in self.releases])

    @cached_property
    def mode_track_count(self) -> int:
        lst = [r.track_count for r in self.releases]
        return max(set(lst), key=lst.count)

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
    def normal_releases(self) -> list["Release"]:
        return [r for r in self.releases if self._is_normal(r)]

    def _is_normal(self, r: "Release") -> bool:
        #script
        if r.script is not None and r.script not in ["Latin"]:
            # _logger.debug(f"normal releases: wrong script ({r.script}) for release {r}")
            return False
        #format
        # for m in r.mediums:
        #     if m.format_id not in []:
        #         return False

        #year
        if self.first_release_date is not None:
            if r.first_release_date is not None and self.first_release_date - r.first_release_date > 3 * datetime.timedelta(
                    days=365):
                # _logger.debug(f"normal releases: late release, more than 3 years after initial release")
                return False

        #track amount
        if r.track_count > self.mode_track_count:
            # _logger.debug(f"normal release: too many track {r.track_count} vs {self.min_track_count} for release {r}")
            return False

        return True

    @cached_property
    def extended_releases(self) -> list["Release"]:
        return [r for r in self.releases if self._is_extended(r)]

    def _is_extended(self, r: "Release") -> bool:
        if r.script is not None and r.script not in ["Latin"]:
            # _logger.debug(f"normal releases: wrong script ({r.script}) for release {r}")
            return False
        #format
        # for m in r.mediums:
        #     if m.format_id not in []:
        #         return False

        #track amount
        if r.track_count < self.mode_track_count:
            # >_logger.debug(f"normal release: too many track {r.track_count} vs {self.min_track_count} for release {r}")
            return False

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
    def normal_recordings(self) -> list["Recording"]:
        result = []
        for rel in self.normal_releases:
            for rec in rel.recordings:
                if not rec in result:
                    result.append(rec)
        return result

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
            _logger.debug(f"{self} is not a sane candidate for artist {artist_query}")
        title_ratio = rapidfuzz.process.extractOne(
            flatten_title(album_name=title_query),
            [flatten_title(album_name=self.title)] + [flatten_title(album_name=x) for x in self.aliases],
            processor=rapidfuzz.utils.default_process
        )[1]
        if title_ratio < cut_off:
            _logger.debug(f"{self} is not a sane candidate for title {title_query}")
        return artist_ratio > cut_off and title_ratio > cut_off

    @cached_property
    def external_urls(self) -> dict[str, list[str]]:
        out = {}
        with get_db_session() as session:
            stmt = (
                sa.select(mbdata.models.LinkReleaseGroupURL)
                .options(
                    sa.orm.selectinload(mbdata.models.LinkReleaseGroupURL.url))
                .options(
                    sa.orm.selectinload(mbdata.models.LinkReleaseGroupURL.link)
                    .selectinload(mbdata.models.Link.link_type)
                )

                .where(mbdata.models.LinkReleaseGroupURL.release_group_id == self._db_id)
            )
            lrgus = session.scalars(stmt).all()
            lrgu: mbdata.models.LinkReleaseGroupURL
            for lrgu in lrgus:
                url: str = str(lrgu.url.url)
                urltype: str = str(lrgu.link.link_type.link_phrase)
                if urltype not in out.keys():
                    out[urltype] = [url]
                else:
                    out[urltype].append(url)

        return out

    @cached_property
    def discogs_ids(self) -> list[tuple[str, int]]:
        if "Discogs" in self.external_urls.keys():
            urls = [u.rsplit('/')[-1] for u in self.external_urls["Discogs"]]
            ids = [('master', int(u)) for u in urls]
            return ids
        return []

    @cached_property
    def spotify_link(self) -> list[str]:
        urls = []
        if "stream {video} for free" in self.external_urls.keys():
            urls = [u for u in self.external_urls["stream {video} for free"] if "open.spotify.com" in u]
        return urls

    @cache
    def find_any_spotify_link(self) -> list[str]:
        urls = []
        urls += self.spotify_link
        if len(urls) > 0:
            return urls
        for rels in self.normal_releases:
            rels_urls = rels.spotify_link
            for rels_url in rels_urls:
                if rels_urls not in urls:
                    urls.append(rels_url)
        if len(urls) > 0:
            return urls
        for rels in self.extended_releases:
            rels_urls = rels.spotify_link
            for rels_url in rels_urls:
                if rels_urls not in urls:
                    urls.append(rels_url)
        return urls

    def __str__(self):
        s1 = f" [{self.primary_type}]" if self.primary_type is not None else ""
        s2 = (
            f" {self.first_release_date}" if self.first_release_date is not None else ""
        )
        return f"'{self.artist_credit_phrase}' - '{self.title}'{s1}{s2} [{self.id}]"

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
            self.script: str = rel.script.name if rel.script is not None else None

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

    def is_years_older_than(self, other: "Release|int") -> Optional[int]:
        if isinstance(other, Release):
            if self.first_release_date is None or other.first_release_date is None:
                return None
            delta = other.first_release_date - self.first_release_date
            years = (delta.days + 1 if delta.days < 0 else delta.days) // 365
            # _logger.debug(f"{self} is {years} years older than {other}")
            return years + 1 if years < 0 else years
        elif isinstance(other, int):
            if self.first_release_date is None:
                return None
            return other - self.first_release_date.year
        else:
            raise NotImplementedError

    @cached_property
    def release_group(self) -> ReleaseGroup:
        from .object_cache import get_release_group
        return get_release_group(self._release_group_id)

    @cached_property
    def is_normal_release(self) -> bool:
        return self in self.release_group.normal_releases

    @cached_property
    def is_extended_release(self) -> bool:
        return self in self.release_group.extended_releases

    @cached_property
    def mediums(self) -> list["Medium"]:
        from .object_cache import get_medium
        with get_db_session() as session:
            stmt = sa.select(mbdata.models.Medium).where(mbdata.models.Medium.release_id == str(self._db_id))
            ms: list[mbdata.models.Medium] = session.scalars(stmt).all()

            return [get_medium(m) for m in ms]

    @cached_property
    def track_count(self) -> int:
        return sum([m.track_count for m in self.mediums])

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

    def is_latin(self) -> bool:
        if self.script is None:
            #_logger.debug("No known script, assuming Latin")
            return True
        return self.script == "Latin"

    def is_sane(self, artist_query: str, title_query: str, cut_off=70) -> bool:
        from .util import flatten_title
        artist_ratio = rapidfuzz.fuzz.WRatio(
            flatten_title(artist_name=self.artist_credit_phrase),
            flatten_title(artist_name=artist_query),
            processor=rapidfuzz.utils.default_process,
            score_cutoff=cut_off
        )
        if artist_ratio < cut_off:
            _logger.debug(f"{self} is not a sane candidate for artist {artist_query}")
        title_ratio = rapidfuzz.process.extractOne(
            flatten_title(recording_name=title_query),
            [flatten_title(recording_name=self.title)] + [flatten_title(recording_name=x) for x in self.aliases],
            processor=rapidfuzz.utils.default_process
        )[1]
        if title_ratio < cut_off:
            _logger.debug(f"{self} is not a sane candidate for title {title_query}")
        return artist_ratio > cut_off and title_ratio > cut_off

    @cached_property
    def external_urls(self) -> dict[str, list[str]]:
        out = {}
        with get_db_session() as session:
            stmt = (
                sa.select(mbdata.models.LinkReleaseURL)
                .options(
                    sa.orm.selectinload(mbdata.models.LinkReleaseURL.url))
                .options(
                    sa.orm.selectinload(mbdata.models.LinkReleaseURL.link)
                    .selectinload(mbdata.models.Link.link_type)
                )

                .where(mbdata.models.LinkReleaseURL.release_id == self._db_id)
            )
            lrus = session.scalars(stmt).all()
            lru: mbdata.models.LinkReleaseGroupURL
            for lru in lrus:
                url: str = lru.url.url
                urltype: str = lru.link.link_type.link_phrase
                if urltype not in out.keys():
                    out[urltype] = [url]
                else:
                    out[urltype].append(url)

        return out

    @cached_property
    def discogs_ids(self) -> list[tuple[str, int]]:
        ids = []
        if "Discogs" in self.external_urls.keys():
            urls = [u.rsplit('/')[-1] for u in self.external_urls["Discogs"]]
            ids = [('release', int(u)) for u in urls]

        ids += self.release_group.discogs_ids
        return ids

    @cached_property
    def spotify_link(self) -> list[str]:
        urls = []
        if "stream {video} for free" in self.external_urls.keys():
            urls = [u for u in self.external_urls["stream {video} for free"] if "open.spotify.com" in u]
        urls += self.release_group.spotify_link
        return urls

    def has_spotify_link(self) -> bool:
        return len(self.spotify_link) > 0

    def __str__(self):
        s1 = (f" [{self.countries[0]}]" if len(self.countries) == 1 else
              (f" [{self.countries[0]}+{len(self.countries)}]" if len(self.countries) > 1 else "")
              )
        s2 = (
            f" {self.first_release_date}" if self.first_release_date is not None else ""
        )
        return f"'{self.artist_credit_phrase}' - '{self.title}'{s2}{s1} [{self.id}]"

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
            if self.release_group != other.release_group:
                return self.release_group < other.release_group

            #deprioritize latin
            if self.is_latin() and not other.is_latin():
                return True
            elif other.is_latin() and not self.is_latin():
                return False

            diff = self.is_years_older_than(other)
            if diff is not None:
                return diff > 2

            # prioritize release with spotify link
            if self.has_spotify_link() and not other.has_spotify_link():
                return True
            elif other.has_spotify_link and not self.has_spotify_link():
                return False

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

    # positive: other is newer, negative: self is newer
    def is_years_older_than(self, other: "Recording|int") -> Optional[int]:
        if isinstance(other, Recording):
            if self.first_release_date is None or other.first_release_date is None:
                return None
            delta = other.first_release_date - self.first_release_date
            years = (delta.days + 1 if delta.days < 0 else delta.days) // 365
            #_logger.debug(f"{self} is {years} years older than {other}")
            return years + 1 if years < 0 else years
        elif isinstance(other, int):
            if self.first_release_date is None:
                return None
            return other - self.first_release_date.year
        else:
            raise NotImplementedError

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

    def is_title_alias(self, title) -> bool:
        from .util import flatten_title
        y = flatten_title(recording_name=title)
        return any([y == flatten_title(recording_name=x) for x in self.aliases])

    def is_artist_alias(self, artist) -> bool:
        from .util import flatten_title
        if flatten_title(artist_name=artist) == flatten_title(artist_name=self.artist_credit_phrase):
            return True

        return any([a.is_alias(artist) for a in self.artists])

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

                [types.append(PerformanceWorkAttributes(att.attribute_type.name)) for att in res2 if
                 PerformanceWorkAttributes(att.attribute_type.name) not in types]

            self.performance_type = types

        return ws

    @cached_property
    def is_acapella(self) -> bool:
        return PerformanceWorkAttributes.ACAPPELLA in self.performance_type

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

    @cached_property
    def release_groups(self) -> list[ReleaseGroup]:
        with get_db_session() as session:
            stmt = (
                sa.select(mbdata.models.ReleaseGroup).distinct()
                .join(mbdata.models.Release)
                .join(mbdata.models.Medium)
                .join(mbdata.models.Track)
                .where(mbdata.models.Track.recording_id == self._db_id)
            )
            db_result: list[mbdata.models.ReleaseGroup] = session.scalars(stmt)

            from pymusicbrainz import get_release_group
            result = [get_release_group(rg) for rg in db_result]
        return result

    @cached_property
    def studio_albums(self) -> list[ReleaseGroup]:
        return [rg for rg in self.release_groups if rg.is_studio_album and not rg.is_va]

    @cached_property
    def live_albums(self) -> list[ReleaseGroup]:
        return [rg for rg in self.release_groups if rg.is_live_album and not rg.is_va]

    @cached_property
    def singles(self) -> list[ReleaseGroup]:
        return [rg for rg in self.release_groups if rg.is_single]

    @cached_property
    def eps(self) -> list[ReleaseGroup]:
        return [rg for rg in self.release_groups if rg.is_eps]

    @cached_property
    def soundtracks(self) -> list[ReleaseGroup]:
        return [rg for rg in self.release_groups if rg.is_soundtrack]

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

    def __eq__(self, other):
        if isinstance(other, Recording):
            return self.id == other.id
        else:
            return False

    def __lt__(self, other):
        if isinstance(other, Recording):
            diff = self.is_years_older_than(other)
            if diff is not None:
                return diff > 2
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

    def is_sane(self, artist_query: str | Artist, title_query: str, cut_off=70) -> bool:
        from .util import flatten_title
        if isinstance(artist_query, Artist):
            artist_sane = (artist_query in self.artists)
        else:

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

    @cached_property
    def url(self) -> str:
        return f"https://musicbrainz.org/recording/{self.id}"

    @cached_property
    def external_urls(self) -> dict[str, str]:
        out = {}
        with get_db_session() as session:
            stmt = (
                sa.select(mbdata.models.LinkRecordingURL)
                .options(
                    sa.orm.selectinload(mbdata.models.LinkRecordingURL.url))
                .options(
                    sa.orm.selectinload(mbdata.models.LinkRecordingURL.link)
                    .selectinload(mbdata.models.Link.link_type)
                )

                .where(mbdata.models.LinkRecordingURL.recording_id == self._db_id)
            )
            lrus = session.scalars(stmt).all()
            lru: mbdata.models.LinkReleaseGroupURL
            for lru in lrus:
                url = lru.url.url
                urltype = lru.link.link_type.link_phrase
                if urltype not in out.keys():
                    out[urltype] = [url]
                else:
                    out[urltype].append(url)

        return out

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
            if m.format is None:
                self.format = None
                self.format_id = None
            else:
                self.format = m.format.name
                self.format_id = m.format_id

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
                if tr is None:
                    raise MBIDNotExistsError(f"No Track with ID '{str(in_obj)}'")


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

    def is_years_older_than(self, other: "Track") -> Optional[int]:
        return self.recording.is_years_older_than(other.recording)

    def __lt__(self, other):
        if isinstance(other, Track):
            if self.release == other.release:
                return self.position < other.position
            else:
                return self.release < other.release

    def __str__(self):
        return f"{self.position}/{self.medium.track_count} of '{self.release.artist_credit_phrase}' - '{self.release.title}': '{self.recording.artist_credit_phrase}' - '{self.recording.title}'" + (
            f" [{self.release.release_group.first_release_date.year}]" if self.release.release_group.first_release_date is not None else "")

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
                self.release, self.track = find_track_release_for_release_group_recording(self.release_group,
                                                                                          self.recording)
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
            _logger.debug(f"Git a strange combination of {self.release} with {self.release_group}. Fixing.")
            self.release_group = self.release.release_group

    def is_years_older_than(self, other: "MusicbrainzSingleResult") -> Optional[int]:
        return self.recording.is_years_older_than(other.recording)

    def __repr__(self):
        return self.track.__repr__()

    def __lt__(self, other):
        if isinstance(other, MusicbrainzSingleResult):
            if self.release_group == other.release_group:
                if self.release == other.release:
                    return self.track < other.track
                else:
                    return self.release < other.release
            else:
                return self.release < other.release

    def __eq__(self, other):
        if isinstance(other, MusicbrainzSingleResult):
            return self.release_group == other.release_group and self.recording == other.recording


class MusicbrainzListResult(list[MusicbrainzSingleResult]):
    def sort(self, *, key=None, reverse=False, live: bool = False, year: int = None):
        if live:
            if year is None:
                super().sort(key=lambda x: (x.recording.is_live, x))
            else:
                super().sort(key=lambda x: (
                    x.recording.is_live, _abs_for_none(x.release_group.is_years_older_than(year)),
                    _abs_for_none(x.recording.is_years_older_than(year)), x))
        else:
            if year is None:
                super().sort()
            else:
                super().sort(key=lambda x: (_abs_for_none(x.recording.is_years_older_than(year)), x))


class MusicbrainzSearchResult:

    def __init__(self, live: bool = False, year: int = None):
        self._best_result_type = None
        self._dict: dict[SearchType, MusicbrainzListResult] = {}
        self.live = live
        self.year = year  # year of release if known

    def add_result(self, search_type: SearchType, result: MusicbrainzListResult) -> None:
        self._dict[search_type] = result

    def get_result(self, search_type: SearchType) -> Optional[MusicbrainzSingleResult]:
        if search_type in self._dict.keys() and len(self._dict[search_type]) > 0:
            self._dict[search_type].sort(live=self.live)
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

    @property
    def compilation(self) -> Optional[MusicbrainzSingleResult]:
        return self.get_result(SearchType.COMPILATION)

    @property
    def extended_album(self) -> Optional[MusicbrainzSingleResult]:
        return self.get_result(SearchType.EXTENDED_ALBUM)

    @property
    def manual(self) -> Optional[MusicbrainzSingleResult]:
        return self.get_result(SearchType.MANUAL)

    @property
    def imported(self) -> Optional[MusicbrainzSingleResult]:
        return self.get_result(SearchType.IMPORT)

    def iterate_results(self) -> Generator[SearchType, MusicbrainzSingleResult]:
        for search_type in SearchType:
            r = self.get_result(SearchType(search_type))
            if r is not None:
                yield search_type, r

    def get_best_result(self) -> Optional[MusicbrainzSingleResult]:

        if self.is_empty():  # something exists
            raise NotFoundError("Result is empty")

        choice = None

        if self.manual is not None:
            choice = SearchType.MANUAL

        elif self.imported is not None:
            choice = SearchType.IMPORT

        elif self.studio_album is not None:  # there may be no canonical
            choice = SearchType.STUDIO_ALBUM
            if self.soundtrack is not None:
                if self.soundtrack < self.studio_album:
                    choice = SearchType.SOUNDTRACK

        elif self.ep is not None:  # there is no album
            choice = SearchType.EP
            if self.soundtrack is not None:
                if self.soundtrack < self.ep:
                    choice = SearchType.SOUNDTRACK

        elif self.soundtrack is not None:  # there is no ep
            choice = SearchType.SOUNDTRACK
            if self.single is not None:
                if self.single < self.soundtrack:
                    choice = SearchType.SINGLE

        elif self.single is not None:
            choice = SearchType.SINGLE

        elif self.compilation is not None:
            choice = SearchType.COMPILATION

        elif self.extended_album is not None:
            choice = SearchType.EXTENDED_ALBUM

        elif self.canonical is not None:
            choice = SearchType.CANONICAL

        elif self.all is not None:
            _logger.debug("No other release found, but found something outside my predefined categories")
            choice = SearchType.ALL

        # should never get here
        if choice is None:
            raise NotFoundError("Was not able to determine a best result for non-empy result set")

        self._best_result_type = choice
        return self.get_result(choice)

    @property
    def best_result(self) -> MusicbrainzSingleResult:
        return self.get_best_result()

    @property
    def best_result_type(self) -> SearchType:
        self.get_best_result()
        return self._best_result_type

    def is_best_result_type(self, searchtype: SearchType) -> bool:
        return self.best_result_type == searchtype

    def __repr__(self):
        best_result = self.get_best_result()
        best_track = best_result.track
        return "(Search result) best result:" + best_track.__repr__() + "  of type " + self.best_result_type

    @classmethod
    def result_from_recording(cls, recording: Recording, canonical_result: Optional[MusicbrainzListResult] = None,
                              year: Optional[int] = None) -> "MusicbrainzSearchResult":
        from pymusicbrainz import search_song_canonical

        result = MusicbrainzSearchResult(live=recording.is_live, year=year)

        if canonical_result is None:
            canonical_result = search_song_canonical(recording.artist_credit_phrase, recording.title,
                                                     live=recording.is_live)
        result.add_result(search_type=SearchType.CANONICAL, result=canonical_result)

        if recording.is_live:
            album_options = recording.studio_albums + recording.live_albums
        else:
            album_options = recording.studio_albums
        if len(album_options) > 0:
            album_result = MusicbrainzListResult(
                [MusicbrainzSingleResult(release_group=x, recording=recording) for x in album_options])
            result.add_result(search_type=SearchType.STUDIO_ALBUM, result=album_result)

        if len(recording.soundtracks) > 0:
            soundtrack_result = MusicbrainzListResult(
                [MusicbrainzSingleResult(release_group=x, recording=recording) for x in recording.soundtracks])
            result.add_result(search_type=SearchType.SOUNDTRACK, result=soundtrack_result)

        if len(recording.eps) > 0:
            ep_result = MusicbrainzListResult(
                [MusicbrainzSingleResult(release_group=x, recording=recording) for x in recording.eps])
            result.add_result(search_type=SearchType.EP, result=ep_result)

        if len(recording.singles) > 0:
            single_result = MusicbrainzListResult(
                [MusicbrainzSingleResult(release_group=x, recording=recording) for x in recording.singles])
            result.add_result(search_type=SearchType.SINGLE, result=single_result)

        return result


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
