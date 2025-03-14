from __future__ import annotations

import logging
from abc import ABC
from dataclasses import dataclass, field

from typing import Optional

import rapidfuzz

from . import identifiers, factory, util


@dataclass
class MBDataObject(ABC):
    id: identifiers.MBID
    factory: factory.MBFactory

    @property
    def type(self) -> str:
        return self.__class__.__name__.lower()

    @property
    def url(self) -> str:
        return f"https://musicbrainz.org/{self.type}/{self.id}"


@dataclass
class Artist(MBDataObject):
    id: identifiers.ArtistID

    name: str
    sort_name: str
    disambiguation: str

    _db_id: Optional[str] = None
    aliases: list[str] = field(default_factory=list)
    artist_type: Optional[str] = None
    country: Optional[str] = None

    _logger: logging.Logger = logging.getLogger(__name__)

    @property
    def release_groups(self) -> list["ReleaseGroup"]:
        raise NotImplementedError

    @property
    def albums(self) -> list["ReleaseGroup"]:
        raise NotImplementedError

    @property
    def singles(self) -> list["ReleaseGroup"]:
        raise NotImplementedError

    @property
    def eps(self) -> list["ReleaseGroup"]:
        raise NotImplementedError

    @property
    def studio_albums(self) -> list["ReleaseGroup"]:
        raise NotImplementedError

    @property
    def soundtracks(self) -> list["ReleaseGroup"]:
        raise NotImplementedError

    def is_sane(self, artist_query: str, cut_off=70) -> bool:

        artist_split = util.split_artist(artist_query)

        artist_ratios = [rapidfuzz.process.extractOne(
            util.flatten_title(artist_name=split),
            [util.flatten_title(self.name)] + [util.flatten_title(a) for a in self.aliases],
            processor=rapidfuzz.utils.default_process
        )[1] for split in artist_split]
        artist_ratio = max(artist_ratios)
        if artist_ratio < cut_off:
            self._logger.debug(f"{self} is not a sane candidate for artist {artist_query}")
        return artist_ratio > cut_off

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


@dataclass
class ReleaseGroup(MBDataObject):
    pass


@dataclass
class Release(MBDataObject):
    pass


@dataclass
class Recording(MBDataObject):
    pass


@dataclass
class Medium(MBDataObject):
    pass


@dataclass
class Track(MBDataObject):
    pass


@dataclass
class Work(MBDataObject):
    pass
