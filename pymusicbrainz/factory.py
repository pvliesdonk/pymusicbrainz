from __future__ import annotations

import datetime
import pathlib
import shelve
from abc import ABC, abstractmethod
from functools import lru_cache
from typing import MutableMapping, Optional

import mbdata.models
import musicbrainzngs
import sqlalchemy as sa

from . import (
    db,
    util,
    ReleaseType,
    musicbrainz_api,
    PerformanceWorkAttributes,
    VA_ARTIST_ID,
)
from .exceptions import *
from .identifiers import *
from .mbdataclass import (
    Artist,
    ReleaseGroup,
    Release,
    Recording,
    Medium,
    Track,
    Work,
    MBDataObject,
)


class MBFactory(ABC):
    def __init__(self):

        self._main_factory = None
        self._backup_factory = None

    @property
    def backup_factory(self) -> Optional[MBFactory]:
        return self._backup_factory

    @backup_factory.setter
    def backup_factory(self, factory: Optional[MBFactory]):
        self._backup_factory = factory

    @property
    def main_factory(self) -> Optional[MBFactory]:
        if self._main_factory is None:
            return self
        return self._main_factory

    @main_factory.setter
    def main_factory(self, factory: Optional[MBFactory]):
        self._main_factory = factory

    def chain_to(self, factory: MBFactory):
        self.backup_factory = factory
        if self.main_factory is None:
            factory.main_factory = self
        else:
            factory.main_factory = self.main_factory

    @property
    def factory_chain(self) -> list[MBFactory]:
        if self.main_factory is None:
            factory = self
        else:
            factory = self.main_factory
        factories = []
        while factory is not None:
            factories.append(factory)
            factory = factory.backup_factory
        return factories

    @staticmethod
    def get_factory(shelf_file: pathlib.Path = None) -> MBFactory:
        cache_factory = CacheFactory(shelf_file=shelf_file)  # main
        api_factory = APIFactory()  # final backup

        try:
            db_factory = DBFactory()
            cache_factory.chain_to(db_factory)
            db_factory.chain_to(api_factory)
        except FactoryNotAvailable as ex:
            logging.getLogger(__name__).debug(
                "Database not available. Instantiated only an APIFactory"
            )
            cache_factory.chain_to(api_factory)
        logging.getLogger(__name__).debug(
            f"Instantiated factory chain: {' --> '.join([str(f) for f in cache_factory.factory_chain])}"
        )
        return cache_factory

    def get_object_from_id(self, id: MBID) -> MBDataObject:
        if isinstance(id, ArtistID):
            return self.get_artist(id)
        elif isinstance(id, ReleaseGroupID):
            return self.get_release_group(id)
        elif isinstance(id, ReleaseID):
            return self.get_release(id)
        elif isinstance(id, RecordingID):
            return self.get_recording(id)
        elif isinstance(id, WorkID):
            return self.get_work(id)
        elif isinstance(id, TrackID):
            return self.get_track(id)
        else:
            raise NotFoundError(f"Could not identify musicbrainz id {id}")

    @abstractmethod
    def get_artist(self, in_obj: ArtistID | str | uuid.UUID) -> Artist:
        pass

    @abstractmethod
    def get_release_group(
        self, in_obj: ReleaseGroupID | str | uuid.UUID
    ) -> ReleaseGroup:
        pass

    @abstractmethod
    def get_release(self, in_obj: ReleaseID | str | uuid.UUID) -> Release:
        pass

    @abstractmethod
    def get_recording(self, in_obj: RecordingID | str | uuid.UUID) -> Recording:
        pass

    @abstractmethod
    def get_medium_from_release(self, in_obj: Release) -> Medium:
        pass

    @abstractmethod
    def get_medium_from_track(self, in_obj: Track) -> Medium:
        pass

    @abstractmethod
    def get_track(self, in_obj: TrackID | str | uuid.UUID) -> Track:
        pass

    @abstractmethod
    def get_work(self, in_obj: WorkID | str | uuid.UUID) -> Work:
        pass

    def update_mbid(self, mbid: MBID) -> MBID:
        if isinstance(mbid, ArtistID):
            return self.update_artist_id(mbid)
        elif isinstance(mbid, ReleaseGroupID):
            return self.update_release_group_id(mbid)
        elif isinstance(mbid, ReleaseID):
            return self.update_release_id(mbid)
        elif isinstance(mbid, RecordingID):
            return self.update_recording_id(mbid)
        else:
            raise NotImplementedError

    @abstractmethod
    def update_artist_id(self, mbid: ArtistID) -> ArtistID:
        pass

    @abstractmethod
    def update_release_group_id(self, mbid: ReleaseGroupID) -> ReleaseGroupID:
        pass

    @abstractmethod
    def update_release_id(self, mbid: ReleaseID) -> ReleaseID:
        pass

    @abstractmethod
    def update_recording_id(self, mbid: RecordingID) -> RecordingID:
        pass

    @abstractmethod
    def performance_of_recording(
        self, recording: Recording
    ) -> tuple[list[Work], list[PerformanceWorkAttributes]]:
        pass

    def __str__(self):
        return str(type(self))

    def __repr__(self):
        return f"{type(self).__name__}()"


class CacheFactory(MBFactory):
    _logger: logging.Logger = logging.getLogger(__name__)

    def __init__(
        self,
        shelf_file: pathlib.Path = None,
    ):
        super().__init__()
        if shelf_file is None:
            self._logger.debug(f"Creating CacheFactory backed by dict in memory")
            self._cache: MutableMapping[MBID, MBDataObject] = {}
        else:
            self._logger.debug(f"Creating CacheFactor backed by shelf in {shelf_file}")
            self._cache: shelve.Shelf = shelve.open(str(shelf_file))

    def clear_cache(self):
        if isinstance(self._cache, dict):
            _object_cache: dict[MBID, MBDataObject] = {}
        if isinstance(self._cache, shelve.Shelf):
            self._cache.sync()

    @MBFactory.backup_factory.getter
    def backup_factory(self) -> Optional[MBFactory]:
        if self._backup_factory is None:
            raise FactoryNotAvailable("Cannot use CacheFactory without backup")
        return self._backup_factory

    def get_artist(self, in_obj: ArtistID | str | uuid.UUID) -> Artist:
        a_id = ArtistID(in_obj)
        if a_id not in self._cache:
            self._cache[a_id] = self.backup_factory.get_artist(a_id)
        return self._cache[a_id]

    def get_release_group(
        self, in_obj: ReleaseGroupID | str | uuid.UUID
    ) -> ReleaseGroup:
        rg_id = ReleaseGroupID(in_obj)
        if rg_id not in self._cache:
            self._cache[rg_id] = self.backup_factory.get_release_group(rg_id)
        return self._cache[rg_id]

    def get_release(self, in_obj: ReleaseID | str | uuid.UUID) -> Release:
        rel_id = ReleaseID(in_obj)
        if rel_id not in self._cache:
            self._cache[rel_id] = self.backup_factory.get_release(rel_id)
        return self._cache[rel_id]

    def get_recording(self, in_obj: RecordingID | str | uuid.UUID) -> Recording:
        rec_id = RecordingID(in_obj)
        if rec_id not in self._cache:
            self._cache[rec_id] = self.backup_factory.get_recording(rec_id)
        return self._cache[rec_id]

    def get_medium_from_release(self, in_obj: Release) -> Medium:
        return self.backup_factory.get_medium_from_release(in_obj)

    def get_medium_from_track(self, in_obj: Track) -> Medium:
        return self.backup_factory.get_medium_from_track(in_obj)

    def get_track(self, in_obj: TrackID | str | uuid.UUID) -> Track:
        t_id = TrackID(in_obj)
        if t_id not in self._cache:
            self._cache[t_id] = self.backup_factory.get_track(t_id)
        return self._cache[t_id]

    def get_work(self, in_obj: WorkID | str | uuid.UUID) -> Work:
        w_id = WorkID(in_obj)
        if w_id not in self._cache:
            self._cache[w_id] = self.backup_factory.get_work(w_id)
        return self._cache[w_id]

    def update_artist_id(self, mbid: ArtistID) -> ArtistID:
        return self.backup_factory.update_artist_id(mbid)

    def update_release_group_id(self, mbid: ReleaseGroupID) -> ReleaseGroupID:
        return self.backup_factory.update_release_group_id(mbid)

    def update_release_id(self, mbid: ReleaseID) -> ReleaseID:
        return self.backup_factory.update_release_id(mbid)

    def update_recording_id(self, mbid: RecordingID) -> RecordingID:
        return self.backup_factory.update_recording_id(mbid)

    def performance_of_recording(
        self, recording: Recording
    ) -> tuple[list[Work], list[PerformanceWorkAttributes]]:
        return self.backup_factory.performance_of_recording(recording)


class DBFactory(MBFactory):
    _logger: logging.Logger = logging.getLogger(__name__)

    def __init__(self):
        try:
            session = db.get_db_session()
            connection = session.connection()
        except Exception as ex:
            self._logger.warning(f"Could not connect to database: {ex}")
            raise FactoryNotAvailable()
        super().__init__()

    @lru_cache
    def get_artist(self, in_obj: ArtistID | str | uuid.UUID) -> Artist:
        with db.get_db_session() as session:
            if isinstance(in_obj, str) or isinstance(in_obj, uuid.UUID):
                in_obj = ArtistID(in_obj)

            self._logger.debug(f"Looking up artist {in_obj} in Musicbrainz Database")

            stmt = sa.select(mbdata.models.Artist).where(
                mbdata.models.Artist.gid == str(in_obj)
            )
            a: mbdata.models.Artist = session.scalar(stmt)
            if a is None:
                raise MBIDNotExistsError(f"No Artist with ID '{str(in_obj)}'")

            # aliases
            stmt = sa.select(mbdata.models.ArtistAlias).where(
                mbdata.models.ArtistAlias.artist == a
            )
            result = session.scalars(stmt)
            aliases = [alias.name for alias in result]

            country = util.area_to_country_db(a.area)

            artist = Artist(
                id=ArtistID(str(a.gid)),
                _db_id=a.id,
                name=a.name,
                artist_type=a.type.name if a.type is not None else None,
                sort_name=a.sort_name,
                disambiguation=a.comment,
                aliases=aliases,
                country=country,
                factory=self.main_factory,
            )

            return artist

    @lru_cache
    def get_release_group(
        self, in_obj: ReleaseGroupID | str | uuid.UUID
    ) -> ReleaseGroup:
        with db.get_db_session() as session:
            if isinstance(in_obj, str) or isinstance(in_obj, uuid.UUID):
                in_obj = ReleaseGroupID(in_obj)

            self._logger.debug(
                f"Looking up release group {in_obj} in Musicbrainz Database"
            )

            stmt = sa.select(mbdata.models.ReleaseGroup).where(
                mbdata.models.ReleaseGroup.gid == str(in_obj)
            )
            rg: mbdata.models.ReleaseGroup = session.scalar(stmt)

            if rg is None:
                raise MBIDNotExistsError(f"No Release Group with ID '{str(in_obj)}'")

            artists = [
                self.get_artist(ArtistID(str(a.artist.gid)))
                for a in rg.artist_credit.artists
            ]
            primary_type = ReleaseType(rg.type.name) if rg.type is not None else None
            types = ([primary_type] if primary_type is not None else []) + [
                ReleaseType(s.secondary_type.name) for s in rg.secondary_types
            ]

            # first release
            stmt = sa.select(mbdata.models.ReleaseGroupMeta).where(
                mbdata.models.ReleaseGroupMeta.id == rg.id
            )
            rgm: mbdata.models.ReleaseGroupMeta = session.scalar(stmt)

            first_release_date = util.parse_partial_date(rgm.first_release_date)

            # get aliases
            stmt = sa.select(mbdata.models.ReleaseGroupAlias).where(
                mbdata.models.ReleaseGroupAlias.release_group == rg
            )
            rgas: list[mbdata.models.ReleaseGroupAlias] = session.scalars(stmt).all()
            aliases = [rg.name]
            for rga in rgas:
                if rga.name not in aliases:
                    aliases.append(rga.name)

            release_group = ReleaseGroup(
                id=in_obj,
                _db_id=rg.id,
                factory=self.main_factory,
                title=rg.name,
                artists=artists,
                aliases=aliases,
                primary_type=primary_type,
                types=types,
                disambiguation=rg.comment,
                artist_credit_phrase=rg.artist_credit.name,
                first_release_date=first_release_date,
                is_va=(rg.artist_credit_id == 1),
            )

            return release_group

    @lru_cache
    def get_release(self, in_obj: ReleaseID | str | uuid.UUID) -> Release:
        with db.get_db_session() as session:
            if isinstance(in_obj, str) or isinstance(in_obj, uuid.UUID):
                in_obj = ReleaseID(in_obj)

            self._logger.debug(f"Looking up release {in_obj} in Musicbrainz Database")

            stmt = sa.select(mbdata.models.Release).where(
                mbdata.models.Release.gid == str(in_obj)
            )
            rel: mbdata.models.Release = session.scalar(stmt)

            if rel is None:
                raise MBIDNotExistsError(f"No Release with ID '{str(in_obj)}'")

            artists = [
                self.get_artist(ArtistID(str(a.artist.gid)))
                for a in rel.artist_credit.artists
            ]
            first_release_date: datetime.date = (
                util.parse_partial_date(rel.first_release.date)
                if rel.first_release is not None
                else None
            )
            countries: list[str] = [
                util.area_to_country_db(c.country.area) for c in rel.country_dates
            ]

            # get aliases
            stmt = sa.select(mbdata.models.ReleaseAlias).where(
                mbdata.models.ReleaseAlias.release_id == rel.id
            )
            ras: list[mbdata.models.ReleaseAlias] = session.scalars(stmt).all()

            aliases = [rel.name]
            for ra in ras:
                if ra.name not in aliases:
                    aliases.append(ra.name)

            release = Release(
                id=ReleaseID(str(rel.gid)),
                _db_id=rel.id,
                artists=artists,
                title=rel.name,
                aliases=aliases,
                first_release_date=first_release_date,
                countries=countries,
                release_group_id=ReleaseGroupID(str(rel.release_group.gid)),
                artist_credit_phrase=rel.artist_credit.name,
                disambiguation=rel.comment,
                factory=self.main_factory,
            )

        return release

    @lru_cache
    def get_recording(self, in_obj: RecordingID | str | uuid.UUID) -> Recording:
        with db.get_db_session() as session:
            if isinstance(in_obj, str) or isinstance(in_obj, uuid.UUID):
                in_obj = RecordingID(in_obj)

            self._logger.debug(f"Looking up recording {in_obj} in Musicbrainz Database")

            stmt = sa.select(mbdata.models.Recording).where(
                mbdata.models.Recording.gid == str(in_obj)
            )
            rec: mbdata.models.Recording = session.scalar(stmt)
            if rec is None:
                raise MBIDNotExistsError(f"No recording with id '{in_obj}'")

            first_release_date = (
                util.parse_partial_date(rec.first_release.date)
                if rec.first_release is not None
                else None
            )

            # aliases
            stmt = sa.select(mbdata.models.RecordingAlias).where(
                mbdata.models.RecordingAlias.recording == rec
            )
            ras: list[mbdata.models.RecordingAlias] = session.scalars(stmt).all()
            aliases = [rec.name]
            for ra in ras:
                if ra.name not in aliases:
                    aliases.append(ra.name)

            recording = Recording(
                id=RecordingID(str(rec.gid)),
                _db_id=rec.id,
                artists=[
                    self.get_artist(ArtistID(str(a.artist.gid)))
                    for a in rec.artist_credit.artists
                ],
                title=rec.name,
                artist_credit_phrase=rec.artist_credit.name,
                disambiguation=rec.comment,
                first_release_date=first_release_date,
                aliases=aliases,
                factory=self.main_factory,
            )
        return recording

    def get_medium_from_release(self, in_obj: Release) -> Medium:
        # TODO: Implement
        raise NotImplementedError

    def get_medium_from_track(self, in_obj: Track) -> Medium:
        # TODO: Implement
        raise NotImplementedError

    @lru_cache
    def get_medium(
        self, in_obj: MediumID | str | uuid.UUID | mbdata.models.Medium
    ) -> Medium:
        if isinstance(in_obj, mbdata.models.Medium):
            with db.get_db_session() as session:
                if isinstance(in_obj, mbdata.models.Medium):
                    m: mbdata.models.Medium = session.merge(in_obj)

                medium = Medium(
                    _db_id=m.id,
                    title=m.name,
                    position=m.position,
                    release_id=ReleaseID(str(m.release.gid)),
                    tracks_ids=[TrackID(str(t.gid)) for t in m.tracks],
                    track_count=m.track_count,
                    format=m.format.name if m.format is not None else None,
                    factory=self.main_factory,
                )
                return medium

        else:
            raise NotImplementedError

    @lru_cache
    def get_track(self, in_obj: TrackID | str | uuid.UUID) -> Track:
        with db.get_db_session() as session:
            if isinstance(in_obj, str):
                in_obj = TrackID(in_obj)

            stmt = sa.select(mbdata.models.Track).where(
                mbdata.models.Track.gid == str(in_obj)
            )
            tr: mbdata.models.Track = session.scalar(stmt)

            artists = [
                self.get_artist(ArtistID(str(a.artist.gid)))
                for a in tr.artist_credit.artists
            ]

            medium: Medium = self.get_medium(tr.medium)

            track = Track(
                id=TrackID(str(tr.gid)),
                _db_id=tr.id,
                artists=artists,
                title=tr.name,
                artist_credit_phrase=tr.artist_credit.name,
                position=tr.position,
                number=tr.number,
                length=tr.length,
                medium=medium,
                recording_id=RecordingID(str(tr.recording.gid)),
                factory=self.main_factory,
            )
            return track

    @lru_cache
    def get_work(self, in_obj: WorkID | str | uuid.UUID) -> Work:
        with db.get_db_session() as session:
            if isinstance(in_obj, str) or isinstance(in_obj, uuid.UUID):
                in_obj = WorkID(in_obj)

            self._logger.debug(f"Looking up work {in_obj} in Musicbrainz Database")

            stmt = sa.select(mbdata.models.Work).where(
                mbdata.models.Work.gid == str(in_obj)
            )
            w: mbdata.models.Work = session.scalar(stmt)

            if w is None:
                raise MBIDNotExistsError(f"No Work with ID '{str(in_obj)}'")

            work = Work(
                id=WorkID(str(w.gid)),
                _db_id=w.id,
                title=w.name,
                disambiguation=w.comment,
                type=w.type.name if w.type is not None else None,
                factory=self.main_factory,
            )
            return work

    def update_artist_id(self, mbid: ArtistID) -> ArtistID:
        with db.get_db_session() as session:
            stmt = (
                sa.select(mbdata.models.Artist.gid)
                .join_from(
                    mbdata.models.ArtistGIDRedirect,
                    mbdata.models.Artist,
                    mbdata.models.ArtistGIDRedirect.artist,
                )
                .where(mbdata.models.ArtistGIDRedirect.gid == str(mbid))
            )
            res = session.scalar(stmt)
            if res is None:
                return mbid
            else:
                return ArtistID(str(res))

    def update_release_group_id(self, mbid: ReleaseGroupID) -> ReleaseGroupID:
        with db.get_db_session() as session:
            stmt = (
                sa.select(mbdata.models.ReleaseGroup.gid)
                .join_from(
                    mbdata.models.ReleaseGroupGIDRedirect,
                    mbdata.models.ReleaseGroup,
                    mbdata.models.ReleaseGroupGIDRedirect.release_group,
                )
                .where(mbdata.models.ReleaseGroupGIDRedirect.gid == str(mbid))
            )
            res = session.scalar(stmt)
            if res is None:
                return mbid
            else:
                return ReleaseGroupID(str(res))

    def update_release_id(self, mbid: ReleaseID) -> ReleaseID:
        with db.get_db_session() as session:
            stmt = (
                sa.select(mbdata.models.Release.gid)
                .join_from(
                    mbdata.models.ReleaseGIDRedirect,
                    mbdata.models.Release,
                    mbdata.models.ReleaseGIDRedirect.release,
                )
                .where(mbdata.models.ReleaseGIDRedirect.gid == str(mbid))
            )
            res = session.scalar(stmt)
            if res is None:
                return mbid
            else:
                return ReleaseID(str(res))

    def update_recording_id(self, mbid: RecordingID) -> RecordingID:
        with db.get_db_session() as session:
            stmt = (
                sa.select(mbdata.models.Recording.gid)
                .join_from(
                    mbdata.models.RecordingGIDRedirect,
                    mbdata.models.Recording,
                    mbdata.models.RecordingGIDRedirect.recording,
                )
                .where(mbdata.models.RecordingGIDRedirect.gid == str(mbid))
            )
            res = session.scalar(stmt)
            if res is None:
                return mbid
            else:
                return RecordingID(str(res))

    @lru_cache
    def performance_of_recording(
        self, recording: Recording
    ) -> tuple[list[Work], list[PerformanceWorkAttributes]]:
        with db.get_db_session() as session:
            if recording._db_id is None:
                # TODO implement: determine correct id if recording didn't come from DBFactory.
                raise NotImplementedError

            stmt = sa.select(mbdata.models.LinkRecordingWork).where(
                mbdata.models.LinkRecordingWork.entity0_id == str(recording._db_id)
            )
            res: list[mbdata.models.LinkRecordingWork] = session.scalars(stmt).all()
            if res is None or len(res) == 0:
                return [], []
            else:
                ws = [self.get_work(r.work.gid) for r in res]

            types = []
            for r in res:
                stmt = sa.select(mbdata.models.LinkAttribute).where(
                    mbdata.models.LinkAttribute.link == r.link
                )
                res2: list[mbdata.models.LinkAttribute] = session.scalars(stmt).all()

                [
                    types.append(PerformanceWorkAttributes(att.attribute_type.name))
                    for att in res2
                    if PerformanceWorkAttributes(att.attribute_type.name) not in types
                ]

            return ws, types


class APIFactory(MBFactory):
    _logger: logging.Logger = logging.getLogger(__name__)

    def __init__(self):
        super().__init__()
        if not musicbrainz_api.is_configured_musicbrainzngs():
            self._logger.debug(
                f"Musicbrainzngs library not initialized. Configuring with default values."
            )
            if self.backup_factory is not None:
                self._logger.debug(
                    f"Using factory of type {type(self.backup_factory)} as backup"
                )
            musicbrainz_api.configure_musicbrainzngs()

    def _artist_credit_phrase(self, artist_credit: dict) -> str:
        s: str = ""
        for a in artist_credit:
            s += a["name"]
            if "joinphrase" in a:
                s += a["joinphrase"]
        return s

    def get_artist(self, in_obj: ArtistID | str | uuid.UUID) -> Artist:

        if isinstance(in_obj, str) or isinstance(in_obj, uuid.UUID):
            in_obj = ArtistID(in_obj)

        self._logger.debug(f"Looking up artist {in_obj} via Musicbrainz API.")

        result = musicbrainzngs.get_artist_by_id(id=str(in_obj), includes=["aliases"])

        artist = Artist(
            id=ArtistID(result["id"]),
            _db_id=None,
            name=result["name"],
            artist_type=(result["type"] if "type" in result else None),
            sort_name=result["sort-name"],
            disambiguation=result["disambiguation"],
            aliases=(
                [a["name"] for a in result["aliases"]] if "aliases" in result else []
            ),
            country=result["country"],
            factory=self.main_factory,
        )

        return artist

    def get_release_group(
        self, in_obj: ReleaseGroupID | str | uuid.UUID
    ) -> ReleaseGroup:

        if isinstance(in_obj, str) or isinstance(in_obj, uuid.UUID):
            in_obj = ReleaseGroupID(in_obj)

        self._logger.debug(f"Looking up release group {in_obj} via Musicbrainz API.")

        result = musicbrainzngs.get_release_group_by_id(
            id=str(in_obj),
            includes=["aliases", "artist-credits"],
        )

        first_release_date = util.parse_partial_date(result["first-release-date"])

        va = ArtistID(result["artist-credit"][0]["artist"]["id"]) == VA_ARTIST_ID

        release_group = ReleaseGroup(
            id=in_obj,
            _db_id=None,
            factory=self.main_factory,
            title=result["title"],
            artists=[
                self.main_factory.get_artist(a["artist"]["id"])
                for a in result["artist-credit"]
            ],
            aliases=[result["title"]]
            + ([a["name"] for a in result["aliases"]] if "aliases" in result else []),
            primary_type=ReleaseType(result["primary-type"]),
            types=[ReleaseType(result["primary-type"])]
            + [ReleaseType(s) for s in result["secondary-types"]],
            disambiguation=result["disambiguation"],
            artist_credit_phrase=self._artist_credit_phrase(result["artist-credit"]),
            first_release_date=first_release_date,
            is_va=va,
        )

        return release_group

    def get_release(self, in_obj: ReleaseID | str | uuid.UUID) -> Release:
        # TODO: Implement
        raise NotImplementedError

    def get_recording(self, in_obj: RecordingID | str | uuid.UUID) -> Recording:
        # TODO: Implement
        raise NotImplementedError

    def get_medium_from_release(self, in_obj: Release) -> Medium:
        # TODO: Implement
        raise NotImplementedError

    def get_medium_from_track(self, in_obj: Track) -> Medium:
        # TODO: Implement
        raise NotImplementedError

    def get_track(self, in_obj: TrackID | str | uuid.UUID) -> Track:
        # TODO: Implement
        raise NotImplementedError

    def get_work(self, in_obj: WorkID | str | uuid.UUID) -> Work:
        # TODO: Implement
        raise NotImplementedError

    def update_artist_id(self, mbid: ArtistID) -> ArtistID:
        # TODO: Implement
        raise NotImplementedError

    def update_release_group_id(self, mbid: ReleaseGroupID) -> ReleaseGroupID:
        # TODO: Implement
        raise NotImplementedError

    def update_release_id(self, mbid: ReleaseID) -> ReleaseID:
        # TODO: Implement
        raise NotImplementedError

    def update_recording_id(self, mbid: RecordingID) -> RecordingID:
        # TODO: Implement
        raise NotImplementedError

    def performance_of_recording(
        self, recording: Recording
    ) -> tuple[list[Work], list[PerformanceWorkAttributes]]:
        # TODO: Implement
        raise NotImplementedError
