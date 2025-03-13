import datetime
from functools import cached_property

import mbdata.models
import rapidfuzz
import sqlalchemy as sa

from pymusicbrainz import ReleaseGroupID, get_db_session, ArtistID, Release, ReleaseID, Recording, RecordingID, Medium, \
    Track, Work
from pymusicbrainz.dataclasses_old import _logger, escape
from pymusicbrainz.datatypes import ReleaseType
from pymusicbrainz.exceptions import MBIDNotExistsError


class ReleaseGroup(MusicBrainzObject):

    def __init__(self,
                 in_obj: ReleaseGroupID | mbdata.models.ReleaseGroup | str) -> None:

        from .object_cache import get_artist

        with get_db_session() as session:
            if isinstance(in_obj, mbdata.models.ReleaseGroup):

                rg: mbdata.models.ReleaseGroup = session.merge(in_obj)
            else:
                if isinstance(in_obj, str):
                    in_obj = ReleaseGroupID(in_obj)
                stmt = sa.select(mbdata.models.ReleaseGroup).where(mbdata.models.ReleaseGroup.gid == str(in_obj))
                rg: mbdata.models.ReleaseGroup = session.scalar(stmt)

                if rg is None:
                    raise MBIDNotExistsError(f"No Release Group with ID '{str(in_obj)}'")

            self.id: ReleaseGroupID = ReleaseGroupID(str(rg.gid))
            self._db_id: int = rg.id
            self.artists = [get_artist(ArtistID(str(a.artist.gid))) for a in rg.artist_credit.artists]
            self.title: str = rg.name
            self.primary_type: ReleaseType = ReleaseType(rg.type.name) if rg.type is not None else None
            self.types: list[ReleaseType] = ([self.primary_type] if self.primary_type is not None else []) + [
                ReleaseType(s.secondary_type.name) for s in rg.secondary_types]
            self.disambiguation: str = rg.comment
            self.artist_credit_phrase: str = rg.artist_credit.name
            self.is_va: bool = (rg.artist_credit_id == 1)

    @cached_property
    def url(self) -> str:
        return f"https://musicbrainz.org/release-group/{self.id}"

    @cached_property
    def is_studio_album(self) -> bool:
        return self.primary_type == ReleaseType.ALBUM and len(self.types) == 1

    @cached_property
    def is_single(self) -> bool:
        return self.primary_type == ReleaseType.SINGLE

    @cached_property
    def is_soundtrack(self) -> bool:
        return self.primary_type == ReleaseType.ALBUM and ReleaseType.SOUNDTRACK in self.types

    @cached_property
    def is_compilation(self) -> bool:
        return ReleaseType.COMPILATION in self.types

    @cached_property
    def is_eps(self) -> bool:
        return self.primary_type == ReleaseType.EP

    @cached_property
    def first_release_date(self) -> datetime.date | None:
        from .util import parse_partial_date
        with get_db_session() as session:
            stmt = sa.select(mbdata.models.ReleaseGroupMeta).where(mbdata.models.ReleaseGroupMeta.id == self._db_id)
            rgm: mbdata.models.ReleaseGroupMeta = session.scalar(stmt)

            return parse_partial_date(rgm.first_release_date)

    @cached_property
    def aliases(self) -> list[str]:
        result = [self.title]
        with get_db_session() as session:
            stmt = sa.select(mbdata.models.ReleaseGroupAlias).where(
                mbdata.models.ReleaseGroupAlias.release_group_id == self._db_id)
            rgas: list[mbdata.models.ReleaseGroupAlias] = session.scalars(stmt).all()

            for rga in rgas:
                if rga.name not in result:
                    result.append(rga.name)
        return result

    @cached_property
    def _releases_db_items(self) -> list["import pymusicbrainz.dataclasses.release]:
        with get_db_session() as session:
            stmt = sa.select(mbdata.models.Release).where(mbdata.models.Release.release_group_id == self._db_id)
            releases: list[mbdata.models.Release] = session.scalars(stmt).all()

            return releases

    @cached_property
    def releases(self) -> list["Release"]:
        from .object_cache import get_release
        return sorted([get_release(release) for release in self._releases_db_items])

    @cached_property
    def release_ids(self) -> list["ReleaseID"]:
        return [ReleaseID(str(release.gid)) for release in self._releases_db_items]

    @cached_property
    def _recordings_db_items(self) -> list["mbdata.models.Recording"]:
        with get_db_session() as session:
            stmt = (
                sa.select(mbdata.models.Recording)
                .join(mbdata.models.Track)
                .join(mbdata.models.Medium)
                .join(mbdata.models.Release)
                .where(mbdata.models.Release.release_group.has(id=self._db_id))
            )
            recordings: list[mbdata.models.Recording] = session.scalars(stmt).all()

            return recordings

    @cached_property
    def recordings(self) -> list["Recording"]:
        from .object_cache import get_recording
        return [get_recording(recording) for recording in self._recordings_db_items]

    @cached_property
    def recording_ids(self) -> list["RecordingID"]:
        return [RecordingID(str(recording.gid)) for recording in self._recordings_db_items]

    def is_sane(self, artist_query: str, title_query: str, cut_off=70) -> bool:
        from .util import flatten_title

        artist_ratio = rapidfuzz.fuzz.WRatio(
            flatten_title(artist_name=self.artist_credit_phrase),
            flatten_title(artist_name=artist_query),
            processor=rapidfuzz.utils.default_process,
            score_cutoff=cut_off
        )
        if artist_ratio < cut_off:
            _logger.warning(f"{self} is not a sane candidate for artist {artist_query}")
        title_ratio = rapidfuzz.process.extractOne(
            flatten_title(album_name=title_query),
            [flatten_title(album_name=self.title)] + [flatten_title(album_name=x) for x in self.aliases],
            processor=rapidfuzz.utils.default_process
        )[1]
        if title_ratio < cut_off:
            _logger.warning(f"{self} is not a sane candidate for title {title_query}")
        return artist_ratio > cut_off and title_ratio > cut_off


    def __str__(self):
        s1 = f" [{self.primary_type}]" if self.primary_type is not None else ""
        s2 = (
            f" {self.first_release_date}" if self.first_release_date is not None else ""
        )
        return f"'{self.artist_credit_phrase}' - '{self.title}'{s1}{s2} [{self.id}]"

    def __rich__(self):
        s1 = f" [{self.primary_type}]" if self.primary_type is not None else ""
        s2 = (
            f" {self.first_release_date}" if self.first_release_date is not None else ""
        )
        return f"'{escape(self.artist_credit_phrase)}' - '{escape(self.title)}'{s1}{s2} \[[link={self.url}]{self.id}[/link]\]"

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
            return item.id in self.recording_ids
        if isinstance(item, Medium):
            return item.release.release_group == self
        if isinstance(item, Track):
            return item.release.release_group == self
        if isinstance(item, Work):
            raise NotImplementedError

    def __hash__(self):
        return hash(self.id)
