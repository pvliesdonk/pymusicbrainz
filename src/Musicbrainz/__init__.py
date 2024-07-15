import logging

from .datatypes import (ArtistID, ReleaseGroupID, ReleaseID, RecordingID, WorkID, MediumID, TrackID,
                        ReleaseType, ReleaseStatus)
from .api import MBApi
from .dataclasses import Artist, ReleaseGroup, Release, Recording, Medium, Track, Work


logging.getLogger("musicbrainzngs").setLevel(logging.ERROR)


def get_artist(artist_id: ArtistID) -> Artist:
    return MBApi().get_artist_by_id(artist_id)


def get_release_group(release_group_id: ReleaseGroupID) -> ReleaseGroup:
    return MBApi().get_release_group_by_id(release_group_id)


def get_release(release_id: ReleaseID) -> Release:
    return MBApi().get_release_by_id(release_id)


def get_recording(recording_id: RecordingID) -> Recording:
    return MBApi().get_recording_by_id(recording_id)


def get_work(work_id: WorkID) -> Work:
    return MBApi().get_work_by_id(work_id)

