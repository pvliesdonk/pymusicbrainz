from __future__ import annotations

import logging
import pathlib
from abc import ABC, abstractmethod
from functools import cache
from typing import Sequence, Optional, Iterator

from .exceptions import IllegaleRecordingReleaseGroupCombination, NotFoundError
from .identifiers import RecordingID, ArtistID
from .mbdataclass import Artist, ReleaseGroup, Recording, Release, Track
from .musicbrainz_types import SearchType


class Search(ABC):

    @abstractmethod
    def search_song(
        self,
        artist_query: Artist | ArtistID | str,
        title_query: str,
        file: pathlib.Path = None,
        seed_id: RecordingID = None,
        additional_seed_ids: Sequence[RecordingID] = None,
    ):
        pass


class APISearch(Search):
    _logger: logging.Logger = logging.getLogger(__name__)

    pass


class MusicbrainzSingleResult:
    _logger: logging.Logger = logging.getLogger(__name__)

    def __init__(
        self,
        release_group: ReleaseGroup,
        recording: Recording,
        release: Optional[Release] = None,
        track: Optional[Track] = None,
    ):
        self.release_group = release_group
        self.recording = recording
        if release is None:
            try:
                self.release, self.track = (
                    find_track_release_for_release_group_recording(
                        self.release_group, self.recording
                    )
                )
            except IllegaleRecordingReleaseGroupCombination as ex:
                raise ex
        elif track is None:
            try:
                self.release = release
                self.track = find_track_for_release_recording(
                    self.release, self.recording
                )
            except IllegaleRecordingReleaseGroupCombination as ex:
                raise ex
        else:
            self.release = release
            self.track = track

        if self.release.release_group_id != self.release_group.id:
            self._logger.warning(
                f"Git a strange combination of {self.release} with {self.release_group}. Fixing."
            )
            self.release_group = self.release.get_release_group()

    def __repr__(self):
        return self.track.__repr__()

    def __lt__(self, other):
        if isinstance(other, MusicbrainzSingleResult):
            return self.track < other.track

    def __eq__(self, other):
        if isinstance(other, MusicbrainzSingleResult):
            return (
                self.release_group == other.release_group
                and self.recording == other.recording
            )


class MusicbrainzListResult(list[MusicbrainzSingleResult]):
    pass


class MusicbrainzSearchResult:
    _logger: logging.Logger = logging.getLogger(__name__)

    def __init__(self, live: bool = False):
        self._dict: dict[SearchType, MusicbrainzListResult] = {}
        self.live = live

    def add_result(
        self, search_type: SearchType, result: MusicbrainzListResult
    ) -> None:
        self._dict[search_type] = result

    def get_result(self, search_type: SearchType) -> Optional[MusicbrainzSingleResult]:
        if search_type in self._dict.keys() and len(self._dict[search_type]) > 0:
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

    def iterate_results(self) -> Iterator[SearchType, MusicbrainzSingleResult]:
        for search_type in SearchType:
            r = self.get_result(SearchType(search_type))
            if r is not None:
                yield search_type, r

    @cache
    def get_best_result(self) -> Optional[MusicbrainzSingleResult]:

        if self.is_empty():  # something exists
            raise NotFoundError("Result is empty")

        choice = None
        if self.canonical is not None:
            choice = SearchType.CANONICAL

        if self.studio_album is not None:  # there may be no canonical
            if self.studio_album != self.canonical:
                choice = SearchType.STUDIO_ALBUM
            # else keep canonical
            if self.soundtrack is not None:
                if self.soundtrack < self.studio_album:
                    self._logger.debug("Found soundtrack older than studio album")
                    choice = SearchType.SOUNDTRACK
        elif self.ep is not None:  # there is no album
            if self.ep != self.canonical:
                choice = SearchType.EP
            if self.soundtrack is not None:
                if self.soundtrack < self.ep:
                    self._logger.debug("Found soundtrack older than ep")
                    choice = SearchType.SOUNDTRACK

        elif self.soundtrack is not None:  # there is no ep
            if self.soundtrack != self.canonical:
                choice = SearchType.SOUNDTRACK
            if self.single is not None:
                if self.single < self.soundtrack:
                    self._logger.debug("Found single older than soundtrack")
                    choice = SearchType.SINGLE

        elif choice is None and self.single is not None:
            self._logger.debug("No other release found, but Single is available")
            choice = SearchType.SINGLE

        elif choice is None and self.all is not None:
            self._logger.debug(
                "No other release found, but found something outside my predefined categories"
            )
            choice = SearchType.ALL

        # should never get here
        if choice is None:
            raise NotFoundError(
                "Was not able to determine a best result for non-empy result set"
            )
        else:
            self._logger.debug(f"Best Musicbrainz result is of type {str(choice)}")

        return self.get_result(choice)

    def __repr__(self):
        return "(Search result) best result:" + self.get_best_result().track.__repr__()


def find_track_for_release_recording(release: Release, recording: Recording) -> Track:
    potential_results = []
    for track in release.get_tracks():
        if track.recording_id == recording.id:
            potential_results.append(track)
    if len(potential_results) == 0:
        raise IllegaleRecordingReleaseGroupCombination(
            f"Release {release} does not contain Recording {recording}"
        )
    return min(potential_results)


def find_track_release_for_release_group_recording(
    rg: ReleaseGroup, recording: Recording
) -> tuple[Release, Track]:
    potential_results = []
    for r in rg.get_releases():
        for track in r.get_tracks():
            if track.recording_id == recording.id:
                potential_results.append((r, track))
    # do some sorting/selection
    if len(potential_results) == 0:
        raise IllegaleRecordingReleaseGroupCombination(
            f"Release Group {rg} does not contain Recording {recording}"
        )
    return min(potential_results)
