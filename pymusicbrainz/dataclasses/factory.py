import logging
import pathlib
import shelve
import uuid
from typing import Mapping, MutableMapping

from .base import MusicBrainzObject
from .identifiers import ArtistID, ReleaseGroupID, ReleaseID, RecordingID, TrackID, WorkID, MBID
from .artist import Artist


class MusicbrainzDataFactory(object):

    def __init__(self,
                 object_cache: MutableMapping[MBID, MusicBrainzObject] = None) -> None:

        self._logger = logging.getLogger(__name__)

        self._object_cache: MutableMapping[MBID, MusicBrainzObject]

        if object_cache is None:
            self._object_cache = {}
            self._object_cache_backing = "dict"
            self._logger.debug("Created a new object-cache backed by dict in memory.")
        elif isinstance(object_cache, dict):
            self._object_cache = object_cache
            self._object_cache_backing = "dict"
            self._logger.debug("Reusing existing object-cache backed by dict in memory.")
        elif isinstance(object_cache, shelve.Shelf):
            self._object_cache = object_cache
            self._object_cache_backing = "shelf"
            self._logger.debug(f"Reusing existing object-cache backed by shelf.")


    def clear_object_cache(self):
        if isinstance(self._object_cache, dict):
            self._object_cache = {}
        if isinstance(self._object_cache, shelve.Shelf):
            self._object_cache.sync()


    def get_artist(self, in_obj: ArtistID | str | uuid.UUID) -> Artist:
        if isinstance(in_obj, str) or isinstance(in_obj, uuid.UUID):
            in_obj = ArtistID(in_obj)

        if in_obj in self._object_cache:
            return self._object_cache[in_obj]


        #TODO: implement
        raise NotImplementedError

    def get_release_group(self, in_obj: ReleaseGroupID | str | uuid.UUID) -> ReleaseGroup:
        # TODO: implement
        raise NotImplementedError

    def get_release(self, in_obj: ReleaseID | str | uuid.UUID) -> Release:
        # TODO: implement
        raise NotImplementedError

    def get_recording(self, in_obj: RecordingID | str | mbdata.models.Recording | uuid.UUID) -> Recording:
        # TODO: implement
        raise NotImplementedError

    def get_track(self, in_obj: TrackID | str | mbdata.models.Track | uuid.UUID) -> Track:
        # TODO: implement
        raise NotImplementedError

    def get_work(self, in_obj: WorkID | str | mbdata.models.Work | uuid.UUID) -> Work:
        #TODO: implement
        raise NotImplementedError

