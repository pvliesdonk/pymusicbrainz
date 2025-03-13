import logging
from abc import abstractmethod
from functools import cached_property, cache

import mbdata.models
import rapidfuzz
import sqlalchemy as sa

from . import releasegroup, release, recording, medium, track, work, base, factory
from ..datatypes import ArtistID, ReleaseGroupID, ReleaseType, SecondaryTypeList


class Artist(base.MusicBrainzObject):
    _logger: logging.Logger = logging.getLogger(__name__)

    def __init__(self, in_obj: ArtistID | str, factory: "ArtistFactory") -> None:

        if isinstance(in_obj, str):
            in_obj = ArtistID(in_obj)

        self._id = in_obj



    @property
    def id(self) -> ArtistID:
        return self._id

    @property
    def type(self) -> str:
        return "artist"

    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @property
    @abstractmethod
    def sort_name(self) -> str:
        pass

    @property
    @abstractmethod
    def disambiguation(self) -> str:
        pass

    @property
    @abstractmethod
    def artist_type(self) -> str:
        pass

    @property
    @abstractmethod
    def aliases(self) -> list[str]:
        pass

    @property
    @abstractmethod
    def country(self) -> str | None:
        pass

    @abstractmethod
    def get_release_groups(self,
                           primary_type: ReleaseType,
                           secondary_types: SecondaryTypeList,
                           credited: bool,
                           contributing: bool) -> list["releasegroup.ReleaseGroup"]:
        pass

    @abstractmethod
    def get_release_group_ids(self,
                              primary_type: ReleaseType,
                              secondary_types: SecondaryTypeList,
                              credited: bool,
                              contributing: bool) -> list["ReleaseGroupID"]:
        pass

    @property
    def release_groups(self) -> list["releasegroup.ReleaseGroup"]:
        return self.get_release_groups(primary_type=ReleaseType.ALL,
                                       secondary_types=SecondaryTypeList([ReleaseType.ALL]), credited=True,
                                       contributing=False)

    @property
    def release_group_ids(self) -> list["ReleaseGroupID"]:
        return self.get_release_group_ids(primary_type=ReleaseType.ALL,
                                          secondary_types=SecondaryTypeList([ReleaseType.ALL]), credited=True,
                                          contributing=False)

    @property
    def albums(self) -> list["releasegroup.ReleaseGroup"]:
        return self.get_release_groups(primary_type=ReleaseType.ALBUM,
                                       secondary_types=SecondaryTypeList([ReleaseType.ALL]), credited=True,
                                       contributing=False)

    @property
    def album_ids(self) -> list["ReleaseGroupID"]:
        return self.get_release_group_ids(primary_type=ReleaseType.ALBUM,
                                          secondary_types=SecondaryTypeList([ReleaseType.ALL]), credited=True,
                                          contributing=False)

    @property
    def singles(self) -> list["releasegroup.ReleaseGroup"]:
        return self.get_release_groups(primary_type=ReleaseType.SINGLE,
                                       secondary_types=SecondaryTypeList([ReleaseType.ALL]), credited=True,
                                       contributing=False)

    @property
    def single_ids(self) -> list["ReleaseGroupID"]:
        return self.get_release_group_ids(primary_type=ReleaseType.SINGLE,
                                          secondary_types=SecondaryTypeList([ReleaseType.ALL]), credited=True,
                                          contributing=False)

    @property
    def eps(self) -> list["releasegroup.ReleaseGroup"]:
        return self.get_release_groups(primary_type=ReleaseType.EP,
                                       secondary_types=SecondaryTypeList([ReleaseType.ALL]), credited=True,
                                       contributing=False)

    @property
    def ep_ids(self) -> list["ReleaseGroupID"]:
        return self.get_release_group_ids(primary_type=ReleaseType.EP,
                                          secondary_types=SecondaryTypeList([ReleaseType.ALL]), credited=True,
                                          contributing=False)

    @property
    def studio_albums(self) -> list["releasegroup.ReleaseGroup"]:
        return self.get_release_groups(primary_type=ReleaseType.ALBUM,
                                       secondary_types=SecondaryTypeList([ReleaseType.NONE]), credited=True,
                                       contributing=False)

    @property
    def studio_album_ids(self) -> list["ReleaseGroupID"]:
        return self.get_release_groups(primary_type=ReleaseType.ALBUM,
                                       secondary_types=SecondaryTypeList([ReleaseType.NONE]), credited=True,
                                       contributing=False)

    @property
    def soundtracks(self) -> list["releasegroup.ReleaseGroup"]:
        return self.get_release_groups(primary_type=ReleaseType.ALBUM,
                                       secondary_types=SecondaryTypeList([ReleaseType.SOUNDTRACK]),
                                       credited=True, contributing=True)

    @property
    def soundtrack_ids(self) -> list["ReleaseGroupID"]:
        return self.get_release_group_ids(primary_type=ReleaseType.ALBUM,
                                          secondary_types=SecondaryTypeList([ReleaseType.SOUNDTRACK]),
                                          credited=True, contributing=True)

    def is_sane(self, artist_query: str, cut_off=70) -> bool:
        from ..util import split_artist, flatten_title

        artist_split = split_artist(artist_query)

        artist_ratios = [rapidfuzz.process.extractOne(
            flatten_title(artist_name=split),
            [flatten_title(self.name)] + [flatten_title(a) for a in self.aliases],
            processor=rapidfuzz.utils.default_process
        )[1] for split in artist_split]
        artist_ratio = max(artist_ratios)
        if artist_ratio < cut_off:
            self._logger.debug(f"{self} is not a sane candidate for artist {artist_query}")
        return artist_ratio > cut_off

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
        if isinstance(item, release.Release):
            return self in item.artists
        if isinstance(item, releasegroup.ReleaseGroup):
            return self in item.artists
        if isinstance(item, recording.Recording):
            return self in item.artists
        if isinstance(item, medium.Medium):
            return self in item.release.artists
        if isinstance(item, track.Track):
            return self in item.artists
        if isinstance(item, work.Work):
            raise NotImplementedError

    def __hash__(self):
        return hash(self.id)


class ArtistFactory(factory.ObjectFactory):
    pass


class ArtistAPI(Artist):
    pass



class ArtistAPIFactory(ArtistFactory):
    pass


class ArtistDB(Artist):
    """Class representing an artist backed by """

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

    @cached_property
    def country(self) -> str | None:
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
            _logger.debug(f"Found {len(result)} release groups matching criteria")

        return result

    @cache
    def get_release_groups(self,
                           primary_type: ReleaseType,
                           secondary_types: SecondaryTypeList,
                           credited: bool,
                           contributing: bool) -> list["releasegroup.ReleaseGroup"]:
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
    def release_groups(self) -> list["releasegroup.ReleaseGroup"]:
        return self.get_release_groups(primary_type=ReleaseType.ALL,
                                       secondary_types=SecondaryTypeList([ReleaseType.ALL]), credited=True,
                                       contributing=False)

    @cached_property
    def release_group_ids(self) -> list["ReleaseGroupID"]:
        return self.get_release_group_ids(primary_type=ReleaseType.ALL,
                                          secondary_types=SecondaryTypeList([ReleaseType.ALL]), credited=True,
                                          contributing=False)

    @cached_property
    def albums(self) -> list["releasegroup.ReleaseGroup"]:
        return self.get_release_groups(primary_type=ReleaseType.ALBUM,
                                       secondary_types=SecondaryTypeList([ReleaseType.ALL]), credited=True,
                                       contributing=False)

    @cached_property
    def album_ids(self) -> list["ReleaseGroupID"]:
        return self.get_release_group_ids(primary_type=ReleaseType.ALBUM,
                                          secondary_types=SecondaryTypeList([ReleaseType.ALL]), credited=True,
                                          contributing=False)

    @cached_property
    def singles(self) -> list["releasegroup.ReleaseGroup"]:
        return self.get_release_groups(primary_type=ReleaseType.SINGLE,
                                       secondary_types=SecondaryTypeList([ReleaseType.ALL]), credited=True,
                                       contributing=False)

    @cached_property
    def single_ids(self) -> list["ReleaseGroupID"]:
        return self.get_release_group_ids(primary_type=ReleaseType.SINGLE,
                                          secondary_types=SecondaryTypeList([ReleaseType.ALL]), credited=True,
                                          contributing=False)

    @cached_property
    def eps(self) -> list["releasegroup.ReleaseGroup"]:
        return self.get_release_groups(primary_type=ReleaseType.EP,
                                       secondary_types=SecondaryTypeList([ReleaseType.ALL]), credited=True,
                                       contributing=False)

    @cached_property
    def ep_ids(self) -> list["ReleaseGroupID"]:
        return self.get_release_group_ids(primary_type=ReleaseType.EP,
                                          secondary_types=SecondaryTypeList([ReleaseType.ALL]), credited=True,
                                          contributing=False)

    @cached_property
    def studio_albums(self) -> list["releasegroup.ReleaseGroup"]:
        return self.get_release_groups(primary_type=ReleaseType.ALBUM,
                                       secondary_types=SecondaryTypeList([ReleaseType.NONE]), credited=True,
                                       contributing=False)

    @cached_property
    def studio_album_ids(self) -> list["ReleaseGroupID"]:
        return self.get_release_groups(primary_type=ReleaseType.ALBUM,
                                       secondary_types=SecondaryTypeList([ReleaseType.NONE]), credited=True,
                                       contributing=False)

    @cached_property
    def soundtracks(self) -> list["releasegroup.ReleaseGroup"]:
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

    def __str__(self):
        if self.disambiguation is not None:
            return f"{self.name} [{self.id}] ({self.disambiguation})"
        else:
            return f"{self.name} [{self.id}]"

    def __rich__(self):
        if self.disambiguation is not None:
            return f"{escape(self.name)} \[[link={self.url}]{self.id}[/link]\] ({escape(self.disambiguation)})"
        else:
            return f"{escape(self.name)} \[[link={self.url}]{self.id}[/link]\]"

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
