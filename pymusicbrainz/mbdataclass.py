from __future__ import annotations

import datetime
import logging
from abc import ABC
from dataclasses import dataclass, field
from typing import Optional, Iterator

import inflection
import rapidfuzz

from . import factory, util, constants
from .identifiers import (
    MBID,
    ArtistID,
    ReleaseGroupID,
    ReleaseID,
    RecordingID,
    TrackID,
    WorkID,
)
from .musicbrainz_types import ReleaseType, PerformanceWorkAttributes, SecondaryTypeList


@dataclass
class MBDataObject(ABC):
    id: MBID
    factory: factory.MBFactory

    @property
    def type(self) -> str:
        return inflection.dasherize(inflection.underscore(self.__class__.__name__))

    def __repr__(self):
        return f"({self.__class__.__name__}(id={self.id})"

    @property
    def url(self) -> str:
        return f"https://musicbrainz.org/{self.type}/{self.id}"


@dataclass
class Artist(MBDataObject):
    id: ArtistID
    name: str
    sort_name: str
    disambiguation: str

    _db_id: Optional[int] = field(default=None)
    aliases: list[str] = field(default_factory=list)
    artist_type: Optional[str] = field(default=None)
    country: Optional[str] = field(default=None)

    _logger: logging.Logger = logging.getLogger(__name__)

    def __post_init__(self):
        self._release_group_ids = None
        self._album_ids = None
        self._single_ids = None
        self._ep_ids = None
        self._studio_album_ids = None
        self._soundtrack_ids = None

    def get_release_group_ids(self) -> list[ReleaseGroupID]:
        if self._release_group_ids is None:
            self._release_group_ids = self.factory.get_artist_release_group_ids(
                artist=self,
                primary_type=ReleaseType.ALL,
                secondary_types=SecondaryTypeList([ReleaseType.ALL]),
                credited=True,
                contributing=False,
            )
        return self._release_group_ids

    def get_release_groups(self) -> Iterator["ReleaseGroup"]:
        for rg_id in self.get_release_group_ids():
            yield self.factory.get_release_group(rg_id)

    def get_album_ids(self) -> list[ReleaseGroupID]:
        if self._album_ids is None:
            self._album_ids = self.factory.get_artist_release_group_ids(
                artist=self,
                primary_type=ReleaseType.ALBUM,
                secondary_types=SecondaryTypeList([ReleaseType.ALL]),
                credited=True,
                contributing=False,
            )
        return self._album_ids

    def get_albums(self) -> Iterator["ReleaseGroup"]:
        for rg_id in self.get_album_ids():
            yield self.factory.get_release_group(rg_id)

    def get_single_ids(self) -> list[ReleaseGroupID]:
        if self._single_ids is None:
            self._single_ids = self.factory.get_artist_release_group_ids(
                artist=self,
                primary_type=ReleaseType.SINGLE,
                secondary_types=SecondaryTypeList([ReleaseType.ALL]),
                credited=True,
                contributing=False,
            )
        return self._single_ids

    def get_singles(self) -> Iterator["ReleaseGroup"]:
        for rg_id in self.get_single_ids():
            yield self.factory.get_release_group(rg_id)

    def get_ep_ids(self) -> list[ReleaseGroupID]:
        if self._ep_ids is None:
            self._ep_ids = self.factory.get_artist_release_group_ids(
                artist=self,
                primary_type=ReleaseType.EP,
                secondary_types=SecondaryTypeList([ReleaseType.ALL]),
                credited=True,
                contributing=False,
            )
        return self._ep_ids

    def get_eps(self) -> Iterator["ReleaseGroup"]:
        for rg_id in self.get_ep_ids():
            yield self.factory.get_release_group(rg_id)

    def get_studio_album_ids(self) -> list[ReleaseGroupID]:
        if self._studio_album_ids is None:
            self._studio_album_ids = self.factory.get_artist_release_group_ids(
                artist=self,
                primary_type=ReleaseType.ALBUM,
                secondary_types=SecondaryTypeList([ReleaseType.NONE]),
                credited=True,
                contributing=False,
            )
        return self._studio_album_ids

    def get_studio_albums(self) -> Iterator["ReleaseGroup"]:
        for rg_id in self.get_studio_album_ids():
            yield self.factory.get_release_group(rg_id)

    def get_soundtrack_ids(self) -> list[ReleaseGroupID]:
        if self._soundtrack_ids is None:
            self._soundtrack_ids = self.factory.get_artist_release_group_ids(
                artist=self,
                primary_type=ReleaseType.ALBUM,
                secondary_types=SecondaryTypeList([ReleaseType.SOUNDTRACK]),
                credited=True,
                contributing=True,
            )
        return self._soundtrack_ids

    def get_soundtracks(self) -> Iterator["ReleaseGroup"]:
        for rg_id in self.get_soundtrack_ids():
            yield self.factory.get_release_group(rg_id)

    def is_sane(self, artist_query: str, cut_off=70) -> bool:

        artist_split = util.split_artist(artist_query)

        artist_ratios = [
            rapidfuzz.process.extractOne(
                util.flatten_title(artist_name=split),
                [util.flatten_title(self.name)]
                + [util.flatten_title(a) for a in self.aliases],
                processor=rapidfuzz.utils.default_process,
            )[1]
            for split in artist_split
        ]
        artist_ratio = max(artist_ratios)
        if artist_ratio < cut_off:
            self._logger.debug(
                f"{self} is not a sane candidate for artist {artist_query}"
            )
        return artist_ratio > cut_off

    def __str__(self):
        if self.disambiguation is not None:
            return f"{self.name} [{self.id}] ({self.disambiguation})"
        else:
            return f"{self.name} [{self.id}]"

    def __repr__(self):
        return f"Artist(id={self.id}, name={self.name})"

    def __eq__(self, other):
        if isinstance(other, Artist):
            return self.id == other.id
        else:
            return False

    def __lt__(self, other):
        return self.sort_name < other.sort_name

    def __contains__(self, item):
        if isinstance(item, Release):
            return self.id in item.artist_ids
        if isinstance(item, ReleaseGroup):
            return self.id in item.artist_ids
        if isinstance(item, Recording):
            return self.id in item.artist_ids
        if isinstance(item, Medium):
            return self.id in item.get_release().artist_ids
        if isinstance(item, Track):
            return self.id in item.artist_ids
        if isinstance(item, Work):
            # TODO: implement
            raise NotImplementedError  # TODO: Implement

    def __hash__(self):
        return hash(self.id)


@dataclass
class ReleaseGroup(MBDataObject):
    id: ReleaseGroupID
    title: str
    disambiguation: str
    artist_ids: list[ArtistID]
    types: list[ReleaseType]
    artist_credit_phrase: str
    is_va: bool

    release_ids: list[ReleaseID]

    primary_type: Optional[ReleaseType] = None
    aliases: list[str] = field(default_factory=list)
    first_release_date: Optional[datetime.date] = None
    _db_id: Optional[int] = field(default=None)
    _logger: logging.Logger = logging.getLogger(__name__)

    def get_artists(self) -> Iterator[Artist]:
        for a in self.artist_ids:
            yield self.factory.get_artist(a)

    @property
    def is_studio_album(self) -> bool:
        return self.primary_type == ReleaseType.ALBUM and len(self.types) == 1

    @property
    def is_single(self) -> bool:
        return self.primary_type == ReleaseType.SINGLE

    @property
    def is_soundtrack(self) -> bool:
        return (
            self.primary_type == ReleaseType.ALBUM
            and ReleaseType.SOUNDTRACK in self.types
        )

    @property
    def is_compilation(self) -> bool:
        return ReleaseType.COMPILATION in self.types

    @property
    def is_eps(self) -> bool:
        return self.primary_type == ReleaseType.EP

    def get_releases(self) -> Iterator[Release]:
        for rel in self.release_ids:
            yield self.factory.get_release(rel)

    def get_recording_ids(self) -> Iterator[RecordingID]:
        yielded = set()
        for rel in self.get_releases():
            for rid in rel.recording_ids:
                if rid not in yielded:
                    yielded.add(rid)
                    yield rid

    def get_recordings(self) -> Iterator[Recording]:
        for rec in self.get_recording_ids():
            yield self.factory.get_recording(rec)

    def is_sane(self, artist_query: str, title_query: str, cut_off=70) -> bool:

        artist_ratio = rapidfuzz.fuzz.WRatio(
            util.flatten_title(artist_name=self.artist_credit_phrase),
            util.flatten_title(artist_name=artist_query),
            processor=rapidfuzz.utils.default_process,
            score_cutoff=cut_off,
        )
        if artist_ratio < cut_off:
            self._logger.warning(
                f"{self} is not a sane candidate for artist {artist_query}"
            )
        title_ratio = rapidfuzz.process.extractOne(
            util.flatten_title(album_name=title_query),
            [util.flatten_title(album_name=self.title)]
            + [util.flatten_title(album_name=x) for x in self.aliases],
            processor=rapidfuzz.utils.default_process,
        )[1]
        if title_ratio < cut_off:
            self._logger.warning(
                f"{self} is not a sane candidate for title {title_query}"
            )
        return artist_ratio > cut_off and title_ratio > cut_off

    def __repr__(self):
        return f"ReleaseGroup(name={self.title}, id={self.id})"

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
            return item.id in self.artist_ids
        if isinstance(item, Release):
            return item.release_group_id == self.id
        if isinstance(item, Recording):
            return item.id in self.get_recording_ids()
        if isinstance(item, Medium):
            return item.get_release().release_group_id == self.id
        if isinstance(item, Track):
            return item.get_release().release_group_id == self.id
        if isinstance(item, Work):
            return any(
                [item.id in rec.get_performances_of() for rec in self.get_recordings()]
            )

    def __hash__(self):
        return hash(self.id)


@dataclass
class Release(MBDataObject):
    id: ReleaseID
    title: str

    release_group_id: ReleaseGroupID
    artist_credit_phrase: str
    disambiguation: str

    artist_ids: list[ArtistID]
    mediums: list[Medium] = field(default_factory=list, init=False)
    first_release_date: Optional[datetime.date] = None
    aliases: list[str] = field(default_factory=list)
    countries: list[str] = field(default_factory=list)
    _logger: logging.Logger = logging.getLogger(__name__)
    _db_id: Optional[int] = field(default=None)

    def get_artists(self) -> Iterator[Artist]:
        for a in self.artist_ids:
            yield self.factory.get_artist(a)

    def is_country_of_artist(self) -> bool:
        return any([a.country in self.countries for a in self.get_artists()])

    @property
    def is_international_release(self) -> bool:
        return any([c in self.countries for c in constants.INT_COUNTRIES])

    @property
    def is_favorite_country(self) -> bool:
        return any([c in self.countries for c in constants.FAVORITE_COUNTRIES])

    def get_release_group(self) -> ReleaseGroup:
        return self.factory.get_release_group(self.release_group_id)

    def get_tracks(self) -> Iterator["Track"]:
        for m in self.mediums:
            yield from m.tracks

    @property
    def recording_ids(self) -> list[RecordingID]:
        return [t.recording_id for t in self.get_tracks()]

    def get_recordings(self) -> Iterator[Recording]:
        for rec in self.recording_ids:
            yield self.factory.get_recording(rec)

    def is_sane(self, artist_query: str, title_query: str, cut_off=70) -> bool:
        artist_ratio = rapidfuzz.fuzz.WRatio(
            util.flatten_title(artist_name=self.artist_credit_phrase),
            util.flatten_title(artist_name=artist_query),
            processor=rapidfuzz.utils.default_process,
            score_cutoff=cut_off,
        )
        if artist_ratio < cut_off:
            self._logger.warning(
                f"{self} is not a sane candidate for artist {artist_query}"
            )
        title_ratio = rapidfuzz.process.extractOne(
            util.flatten_title(recording_name=title_query),
            [util.flatten_title(recording_name=self.title)]
            + [util.flatten_title(recording_name=x) for x in self.aliases],
            processor=rapidfuzz.utils.default_process,
        )[1]
        if title_ratio < cut_off:
            self._logger.warning(
                f"{self} is not a sane candidate for title {title_query}"
            )
        return artist_ratio > cut_off and title_ratio > cut_off

    def __repr__(self):
        return f"Release(name={self.title}, id={self.id})"

    def __str__(self):
        s1 = (
            f" [{self.countries[0]}]"
            if len(self.countries) == 1
            else (
                f" [{self.countries[0]}+{len(self.countries)}]"
                if len(self.countries) > 1
                else ""
            )
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
            return item.id in self.artist_ids
        if isinstance(item, ReleaseGroup):
            return self.release_group_id == item.id
        if isinstance(item, Recording):
            return item.id in self.recording_ids
        if isinstance(item, Medium):
            return item in self.mediums
        if isinstance(item, Track):
            return item in self.get_tracks()
        if isinstance(item, Work):
            return any(
                [item in rec.get_performances_of() for rec in self.get_recordings()]
            )

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
                        # _logger.error("Multiple releases with same date and country:")
                        # _logger.error(self)
                        # _logger.error(other)
                        return True
                else:
                    return True
            else:
                return False

    def __hash__(self):
        return hash(self.id)


@dataclass
class Recording(MBDataObject):
    id: RecordingID
    artist_ids: list[ArtistID]
    title: str
    artist_credit_phrase: str
    disambiguation: str
    performance_type: list[PerformanceWorkAttributes]
    performance_of_ids: list[WorkID]

    first_release_date: Optional[datetime.date] = None
    aliases: list[str] = field(default_factory=list)
    countries: list[str] = field(default_factory=list)
    _logger: logging.Logger = logging.getLogger(__name__)
    _db_id: Optional[int] = field(default=None)

    def __post_init__(self):
        self._performance_type = None
        self._performance_of = None

    def get_artists(self) -> Iterator[Artist]:
        for a in self.artist_ids:
            yield self.factory.get_artist(a)

    def get_performances_of(self) -> Iterator[Work]:
        for p in self.performance_of_ids:
            yield self.factory.get_work(p)

    @property
    def is_acappella(self) -> bool:
        return PerformanceWorkAttributes.ACAPPELLA in self.performance_type

    @property
    def is_live(self) -> bool:
        return PerformanceWorkAttributes.LIVE in self.performance_type

    @property
    def is_medley(self) -> bool:
        return PerformanceWorkAttributes.MEDLEY in self.performance_type

    @property
    def is_partial(self) -> bool:
        return PerformanceWorkAttributes.PARTIAL in self.performance_type

    @property
    def is_instrumental(self) -> bool:
        return PerformanceWorkAttributes.INSTRUMENTAL in self.performance_type

    @property
    def is_cover(self) -> bool:
        return PerformanceWorkAttributes.COVER in self.performance_type

    @property
    def is_karaoke(self) -> bool:
        return PerformanceWorkAttributes.KARAOKE in self.performance_type

    @property
    def is_normal_performance(self) -> bool:
        return len(self.performance_type) == 0

    def get_siblings(self) -> Iterator["Recording"]:
        result = []
        self._logger.debug(f"Computing siblings of {self}")
        works = self.get_performances_of()
        yielded = set()
        for work in works:
            if len(self.performance_type) == 0:
                for r in work.get_performances_by_type(PerformanceWorkAttributes.NONE):
                    if r.id not in yielded and r.artist_ids == self.artist_ids:
                        yielded.add(r.id)
                        yield r
            else:
                self._logger.debug(
                    f"Recording of types {'/'.join(self.performance_type)}; returning matching siblings of {self.artist_credit_phrase} - {self.title}"
                )

                for rec in work.get_performances_by_type(self.performance_type):
                    if rec.artist_ids == self.artist_ids:
                        yielded.add(rec.id)
                        yield rec

    def __repr__(self):
        return f"Recording(name={self.title}, id={self.id})"

    def __str__(self):
        s_date = (
            f" {self.first_release_date}" if self.first_release_date is not None else ""
        )
        return (
            f"'{self.artist_credit_phrase}' - '{self.title}'{s_date} [{self.id}] "
            + (
                "/".join(self.performance_type)
                if len(self.performance_type) > 0
                else ""
            )
        )

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
            return item.id in self.artist_ids
        if isinstance(item, ReleaseGroup):
            return self.id in item.get_recording_ids()
        if isinstance(item, Release):
            return self.id in item.recording_ids
        if isinstance(item, Medium):
            return any([self.id == t.recording_id for t in item.tracks])
        if isinstance(item, Track):
            return item.recording_id == self.id
        if isinstance(item, Work):
            return self.id in item.performance_ids[PerformanceWorkAttributes.ALL]

    def is_sane(self, artist_query: str, title_query: str, cut_off=70) -> bool:
        artist_sane = any(
            [artist.is_sane(artist_query) for artist in self.get_artists()]
        )

        title_ratio = rapidfuzz.process.extractOne(
            util.flatten_title(recording_name=title_query),
            [util.flatten_title(recording_name=self.title)]
            + [util.flatten_title(recording_name=a) for a in self.aliases],
            processor=rapidfuzz.utils.default_process,
        )[1]

        if not artist_sane:
            self._logger.warning(
                f"{self} is not a sane candidate for artist {artist_query}"
            )
        elif title_ratio < cut_off:
            self._logger.warning(
                f"{self} is not a sane candidate for title {title_query}"
            )
        else:
            return True

    def __hash__(self):
        return hash(self.id)


@dataclass
class Medium(object):
    title: str
    position: int
    release_id: ReleaseID
    tracks: list[Track] = field(default_factory=list, init=False)
    track_count: int
    factory: factory.MBFactory

    format: Optional[str] = None

    _logger: logging.Logger = logging.getLogger(__name__)
    _db_id: Optional[int] = field(default=None)

    def __post_init__(self):
        self._tracks = None

    def get_release(self) -> Release:
        return self.factory.get_release(self.release_id)

    def __repr__(self):
        return f"Medium(name={self.title}, release={self.release_id}, position={self.position}, format={self.format})"

    def __str__(self):
        return f"'{self.get_release().artist_credit_phrase}' - '{self.get_release().title}' - '{self.title}'"

    def __contains__(self, item):
        if isinstance(item, Artist):
            return any([item.id in t.artist_ids for t in self.tracks])
        if isinstance(item, ReleaseGroup):
            return self.get_release().release_group_id == item.id
        if isinstance(item, Release):
            return self.release_id == item.id
        if isinstance(item, Recording):
            return any([item.id == t.recording_id for t in self.tracks])
        if isinstance(item, Track):
            return item in self.tracks
        if isinstance(item, Work):
            raise NotImplementedError  # TODO: Implement


@dataclass
class Track(MBDataObject):
    id: TrackID
    artist_ids: list[ArtistID]
    title: str
    artist_credit_phrase: str
    position: int
    number: str
    length: int
    medium: Medium
    recording_id: RecordingID

    _db_id: Optional[int] = field(default=None)
    _logger: logging.Logger = logging.getLogger(__name__)

    @property
    def release_id(self) -> ReleaseID:
        return self.medium.release_id

    def get_artists(self) -> Iterator[Artist]:
        for a in self.artist_ids:
            yield self.factory.get_artist(a)

    def get_recording(self) -> Recording:
        return self.factory.get_recording(self.recording_id)

    def get_release(self) -> Release:
        return self.medium.get_release()

    def __lt__(self, other):
        if isinstance(other, Track):
            if self.medium.release_id == other.medium.release_id:
                return self.position < other.position
            else:
                return self.get_release() < other.get_release()

    def __repr__(self):
        return f"Track(name={self.title}, position={self.position}, recording={self.recording_id})"

    def __str__(self):
        return f"{self.position}/{self.medium.track_count} of '{self.get_release().artist_credit_phrase}' - '{self.get_release().title}': '{self.get_recording().artist_credit_phrase}' - '{self.get_recording().title}'"

    def __contains__(self, item):
        if isinstance(item, Artist):
            return item.id in self.get_recording().artist_ids
        if isinstance(item, ReleaseGroup):
            return self.get_release().release_group_id == item.id
        if isinstance(item, Release):
            return self.medium.release_id == item.id
        if isinstance(item, Medium):
            return self.medium == item
        if isinstance(item, Recording):
            return self.recording_id == item.id
        if isinstance(item, Work):
            return (
                self.recording_id in item.performance_ids[PerformanceWorkAttributes.ALL]
            )


@dataclass
class Work(MBDataObject):
    id: WorkID
    title: str
    disambiguation: str
    performance_ids: dict[PerformanceWorkAttributes, list[RecordingID]]

    work_type: Optional[str] = None
    _db_id: Optional[int] = field(default=None)
    _logger: logging.Logger = logging.getLogger(__name__)

    def get_performances_by_type(
        self, types: list[PerformanceWorkAttributes] | PerformanceWorkAttributes
    ) -> Iterator[Recording]:
        yielded = set()
        if isinstance(types, PerformanceWorkAttributes):
            types = [types]
        for t in types:
            for r in self.performance_ids[t]:
                if r not in yielded:
                    yielded.add(r)
                    yield self.factory.get_recording(r)

    def __repr__(self):
        return f"Work(name={self.title}, id={self.id})"

    def __str__(self):
        return f"{self.title}  [{self.id}]"

    def __eq__(self, other):
        if isinstance(other, Work):
            return self.id == other.id
        else:
            return False

    def __hash__(self):
        return hash(self.id)

    def __contains__(self, item):
        if isinstance(item, Artist):
            raise NotImplementedError  # TODO: Implement
        if isinstance(item, ReleaseGroup):
            raise NotImplementedError  # TODO: Implement
        if isinstance(item, Release):
            raise NotImplementedError  # TODO: Implement
        if isinstance(item, Medium):
            raise NotImplementedError  # TODO: Implement
        if isinstance(item, Track):
            return (
                item.recording_id in self.performance_ids[PerformanceWorkAttributes.ALL]
            )
        if isinstance(item, Recording):
            return item.id in self.performance_ids[PerformanceWorkAttributes.ALL]
