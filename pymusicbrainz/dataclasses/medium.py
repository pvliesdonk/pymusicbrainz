from functools import cached_property

import mbdata.models

from pymusicbrainz import get_db_session, ReleaseID, TrackID, Track, ReleaseGroup, Work
from pymusicbrainz.dataclasses_old import Release, escape, Recording


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

    def __str__(self):
        return (
                f"'{self.release.artist_credit_phrase}' - '{self.release.title}'"
                + (f" - '{self.title}'" if self.title else "")
        )

    def __rich__(self):
        return (
                f"'{escape(self.release.artist_credit_phrase)}' - '{escape(self.release.title)}'"
                + (f" - '{escape(self.title)}'" if self.title else "")
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
