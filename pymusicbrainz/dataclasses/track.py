from functools import cached_property

import mbdata.models
import sqlalchemy as sa

from pymusicbrainz import TrackID, get_db_session, ArtistID, Medium, RecordingID, ReleaseGroup, Work
from pymusicbrainz.dataclasses_old import Recording, Release, escape


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

    def __lt__(self, other):
        if isinstance(other, Track):
            if self.release == other.release:
                return self.position < other.position
            else:
                return self.release < other.release

    def __str__(self):
        return f"{self.position}/{self.medium.track_count} of '{self.release.artist_credit_phrase}' - '{self.release.title}': '{self.recording.artist_credit_phrase}' - '{self.recording.title}'"

    def __rich__(self):
        return f"{self.position}/{self.medium.track_count} of '{escape(self.release.artist_credit_phrase)}' - '{escape(self.release.title)}': '{escape(self.recording.artist_credit_phrase)}' - '{escape(self.recording.title)}'"

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
