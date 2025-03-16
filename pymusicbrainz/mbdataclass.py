from __future__ import annotations

import datetime
import logging
from abc import ABC
from dataclasses import dataclass, field
from typing import Optional

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
from .musicbrainz_types import ReleaseType, PerformanceWorkAttributes


@dataclass
class MBDataObject(ABC):
    id: MBID
    factory: factory.MBFactory

    @property
    def type(self) -> str:
        return inflection.dasherize(inflection.underscore(self.__class__.__name__))

    @property
    def url(self) -> str:
        return f"https://musicbrainz.org/{self.type}/{self.id}"


@dataclass
class Artist(MBDataObject):
    id: ArtistID
    name: str
    sort_name: str
    disambiguation: str

    _db_id: Optional[str] = field(default=None)
    aliases: list[str] = field(default_factory=list)
    artist_type: Optional[str] = field(default=None)
    country: Optional[str] = field(default=None)

    _logger: logging.Logger = logging.getLogger(__name__)

    @property
    def release_groups(self) -> list["ReleaseGroup"]:
        # TODO: implement
        raise NotImplementedError

    @property
    def albums(self) -> list["ReleaseGroup"]:
        # TODO: implement
        raise NotImplementedError

    @property
    def singles(self) -> list["ReleaseGroup"]:
        # TODO: implement
        raise NotImplementedError

    @property
    def eps(self) -> list["ReleaseGroup"]:
        # TODO: implement
        raise NotImplementedError

    @property
    def studio_albums(self) -> list["ReleaseGroup"]:
        # TODO: implement
        raise NotImplementedError

    @property
    def soundtracks(self) -> list["ReleaseGroup"]:
        # TODO: implement
        raise NotImplementedError

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
            # TODO: implement
            raise NotImplementedError

    def __hash__(self):
        return hash(self.id)


@dataclass
class ReleaseGroup(MBDataObject):
    id: ReleaseGroupID
    title: str
    disambiguation: str
    artists: list[Artist]
    types: list[ReleaseType]
    artist_credit_phrase: str
    is_va: bool

    primary_type: Optional[ReleaseType] = None
    aliases: list[str] = field(default_factory=list)
    first_release_date: Optional[datetime.date] = None
    _db_id: Optional[str] = field(default=None)
    _logger: logging.Logger = logging.getLogger(__name__)

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

    @property
    def releases(self) -> list[Release]:
        # TODO: implement
        raise NotImplementedError

    @property
    def recordings(self) -> list[Recording]:
        # TODO: implement
        raise NotImplementedError

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
            return item in self.recordings
        if isinstance(item, Medium):
            return item.release.release_group == self
        if isinstance(item, Track):
            return item.release.release_group == self
        if isinstance(item, Work):
            # TODO: implement
            raise NotImplementedError

    def __hash__(self):
        return hash(self.id)


@dataclass
class Release(MBDataObject):
    id: ReleaseID
    title: str
    artists: list[Artist]
    release_group_id: ReleaseGroupID
    artist_credit_phrase: str
    disambiguation: str

    first_release_date: Optional[datetime.date] = None
    aliases: list[str] = field(default_factory=list)
    countries: list[str] = field(default_factory=list)
    _logger: logging.Logger = logging.getLogger(__name__)
    _db_id: Optional[str] = field(default=None)

    @property
    def is_country_of_artist(self) -> bool:
        return any([a.country in self.countries for a in self.artists])

    @property
    def is_international_release(self) -> bool:
        return any([c in self.countries for c in constants.INT_COUNTRIES])

    @property
    def is_favorite_country(self) -> bool:
        return any([c in self.countries for c in constants.FAVORITE_COUNTRIES])

    @property
    def release_group(self) -> ReleaseGroup:
        return self.factory.get_release_group(self.release_group_id)

    @property
    def mediums(self) -> list["Medium"]:
        # TODO: implement
        raise NotImplementedError

    @property
    def tracks(self) -> list["Track"]:
        # TODO: implement
        raise NotImplementedError

    @property
    def recordings(self) -> list[Recording]:
        # TODO: implement
        raise NotImplementedError

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
            # TODO: implement
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
    artists: list[Artist]
    title: str
    artist_credit_phrase: str
    disambiguation: str

    first_release_date: Optional[datetime.date] = None
    aliases: list[str] = field(default_factory=list)
    countries: list[str] = field(default_factory=list)
    _logger: logging.Logger = logging.getLogger(__name__)
    _db_id: Optional[str] = field(default=None)

    def __post_init__(self):
        self._performance_type = None
        self._performance_of = None

    @property
    def performance_type(self) -> list[PerformanceWorkAttributes]:
        if self._performance_type is None:
            self._performance_of, self._performance_type = (
                self.factory.performance_of_recording(self)
            )
        return self._performance_type

    @property
    def performance_of(self) -> list[Work]:
        if self._performance_of is None:
            self._performance_of, self._performance_type = (
                self.factory.performance_of_recording(self)
            )
        return self._performance_of

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

    @property
    def siblings(self) -> list["Recording"]:
        result = []
        self._logger.debug(f"Computing siblings of {self}")
        works = self.performance_of
        for work in works:
            if len(self.performance_type) == 0:
                for r in work.performance_by_type([PerformanceWorkAttributes.NONE]):
                    if r not in result and r.artists == self.artists:
                        result.append(r)
            else:
                self._logger.debug(
                    f"Recording of types {'/'.join(self.performance_type)}; returning matching siblings of {self.artist_credit_phrase} - {self.title}"
                )

                result = [
                    rec
                    for rec in work.performance_by_type(self.performance_type)
                    if rec.artists == self.artists
                ]
        self._logger.debug(f"Identified {len(result)} siblings")
        return result

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
            return self in item.performances[PerformanceWorkAttributes.ALL]

    def is_sane(self, artist_query: str, title_query: str, cut_off=70) -> bool:
        artist_sane = any([artist.is_sane(artist_query) for artist in self.artists])

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
    tracks_ids: list[TrackID]
    track_count: int
    factory: factory.MBFactory

    format: Optional[str] = None

    id: MediumID = None  # mediums don't have an id.
    _logger: logging.Logger = logging.getLogger(__name__)
    _db_id: Optional[str] = field(default=None)

    @property
    def release(self) -> Release:
        return self.factory.get_release(self.release_id)

    @property
    def tracks(self) -> list[Track]:
        return [self.factory.get_track(t) for t in self.tracks_ids]

    def __str__(self):
        return f"'{self.release.artist_credit_phrase}' - '{self.release.title}'" + (
            f" - '{self.title}'" if self.title else ""
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


@dataclass
class Track(MBDataObject):
    id: TrackID
    artists: list[Artist]
    title: str
    artist_credit_phrase: str
    position: int
    number: str
    length: int
    medium: Medium
    recording_id: RecordingID

    _db_id: Optional[str] = field(default=None)
    _logger: logging.Logger = logging.getLogger(__name__)

    @property
    def recording(self) -> Recording:
        return self.factory.get_recording(self.recording_id)

    @property
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
            return self.recording in item.performances[PerformanceWorkAttributes.ALL]


@dataclass
class Work(MBDataObject):
    id: WorkID
    title: str
    disambiguation: str

    type: Optional[str] = None
    _db_id: Optional[str] = field(default=None)
    _logger: logging.Logger = logging.getLogger(__name__)

    @property
    def performances(self) -> dict[PerformanceWorkAttributes, list[Recording]]:
        # TODO: Implement
        raise NotImplementedError

    def performance_by_type(
        self, types: list[PerformanceWorkAttributes]
    ) -> list[Recording]:
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
            return item.recording in self.performances[PerformanceWorkAttributes.ALL]
        if isinstance(item, Recording):
            return item in self.performances[PerformanceWorkAttributes.ALL]
