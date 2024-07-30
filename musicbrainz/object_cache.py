import mbdata.models

from .dataclasses import Artist, ReleaseGroup, Release, Recording, Track, Work, Medium
from .datatypes import ArtistID, ReleaseGroupID, ReleaseID, RecordingID, TrackID, WorkID
from .exceptions import MBApiError

_object_cache = {}


def clear_object_cache():
    global _object_cache
    _object_cache = {}


def get_artist(in_obj: ArtistID | str | mbdata.models.Artist) -> Artist:
    global _object_cache
    if isinstance(in_obj, mbdata.models.Artist):
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


def get_release_group(in_obj: ReleaseGroupID | str | mbdata.models.ReleaseGroup) -> ReleaseGroup:
    global _object_cache
    if isinstance(in_obj, mbdata.models.ReleaseGroup):
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


def get_release(in_obj: ReleaseID | str | mbdata.models.Release) -> Release:
    global _object_cache
    if isinstance(in_obj, mbdata.models.Release):
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


def get_recording(in_obj: RecordingID | str | mbdata.models.Recording) -> Recording:
    global _object_cache
    if isinstance(in_obj, mbdata.models.Recording):
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


def get_track(in_obj: TrackID | str | mbdata.models.Track) -> Track:
    global _object_cache
    if isinstance(in_obj, mbdata.models.Track):
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


def get_work(in_obj: WorkID | str | mbdata.models.Work) -> Work:
    global _object_cache
    if isinstance(in_obj, mbdata.models.Work):
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


def get_medium(in_obj: mbdata.models.Medium) -> Medium:
    global _object_cache
    if in_obj is not None:
        if in_obj.id in _object_cache.keys():
            return _object_cache[in_obj.id]
        else:
            a = Medium(in_obj)
            _object_cache[in_obj.id] = a
            return a
    else:
        raise MBApiError("No parameters given")
