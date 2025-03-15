from __future__ import annotations

import datetime
import logging
import pathlib
import shelve
import uuid
from abc import ABC, abstractmethod

import musicbrainzngs
import sqlalchemy as sa
import mbdata.models

from . import db, util, ReleaseType, musicbrainz_api, PerformanceWorkAttributes
from .mbdataclass import Artist, ReleaseGroup, Release, Recording, Medium, Track, Work
from .identifiers import *
from .exceptions import *


class MBFactory(ABC):

    def __init__(self, backup_factory: MBFactory = None):
        self.backup_factory = backup_factory

    @staticmethod
    def get_factory(shelf_file: pathlib.Path = None) -> MBFactory:
        api_factory = APIFactory()
        try:
            factory = DBFactory(backup_factory=api_factory)
            logging.getLogger(__name__).debug("Instantiated a DB Factory, using API as backup")
        except FactoryNotAvailable as ex:
            logging.getLogger(__name__).debug("Database not available. Instantiated an APIFactory")
            factory = api_factory

        cache_factory = CacheFactory(
            backup_factory=factory
        )
        return cache_factory

    @abstractmethod
    def get_artist(self, in_obj: ArtistID | str | uuid.UUID) -> Artist:
        pass

    @abstractmethod
    def get_release_group(self, in_obj: ReleaseGroupID | str | uuid.UUID) -> ReleaseGroup:
        pass

    @abstractmethod
    def get_release(self, in_obj: ReleaseID | str | uuid.UUID) -> Release:
        pass

    @abstractmethod
    def get_recording(self, in_obj: RecordingID | str | uuid.UUID) -> Recording:
        pass

    @abstractmethod
    def get_medium(self, in_obj: MediumID | str | uuid.UUID) -> Medium:
        pass

    @abstractmethod
    def get_track(self, in_obj: TrackID | str | uuid.UUID) -> Track:
        pass

    @abstractmethod
    def get_work(self, in_obj: WorkID | str | uuid.UUID) -> Work:
        pass

    @abstractmethod
    def performance_of_recording(self, recording: Recording) -> tuple[list[Work], list[PerformanceWorkAttributes]]:
        pass


class CacheFactory(MBFactory):
    _logger: logging.Logger = logging.getLogger(__name__)

    def __init__(self, backup_factory: MBFactory = None, shelf_file: pathlib.Path = None):
        super().__init__(backup_factory)
        if self.backup_factory is None:
            raise FactoryNotAvailable(f"{type(self)} cannot work with backup factory to cache for.")
        if shelf_file is None:
            self._logger.debug(f"Creating CacheFactory backed by dict in memory")
            self._cache = {}
        else:
            self._logger.debug(f"Creating CacheFactor backed by shelf in {shelf_file}")
            self._cache = shelve.open(str(shelf_file))

    def clear_cache(self):
        if isinstance(self._cache, dict):
            _object_cache = {}
        if isinstance(self._cache, shelve.Shelf):
            self._cache.sync()

    def get_artist(self, in_obj: ArtistID | str | uuid.UUID) -> Artist:
        a_id = ArtistID(in_obj)
        if a_id not in self._cache:
            self._cache[a_id] = self.backup_factory.get_artist(a_id)
        return self._cache[a_id]

    def get_release_group(self, in_obj: ReleaseGroupID | str | uuid.UUID) -> ReleaseGroup:
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

    def get_medium(self, in_obj: MediumID | str | uuid.UUID) -> Medium:
        m_id = MediumID(in_obj)
        if m_id not in self._cache:
            self._cache[m_id] = self.backup_factory.get_medium(m_id)
        return self._cache[m_id]

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

    def performance_of_recording(self, recording: Recording) -> tuple[list[Work], list[PerformanceWorkAttributes]]:
        self.backup_factory.performance_of_recording(recording)


class DBFactory(MBFactory):
    _logger: logging.Logger = logging.getLogger(__name__)

    def __init__(self, backup_factory: MBFactory = None):
        # TODO: check whether database is reachable
        if False:  # database not reachable
            raise FactoryNotAvailable()
        super().__init__(backup_factory)
        if self.backup_factory is not None:
            self._logger.debug(f"Using factory of type {type(self.backup_factory)} as backup")

    def get_artist(self, in_obj: ArtistID | str | uuid.UUID) -> Artist:
        with db.get_db_session() as session:
            if isinstance(in_obj, str) or isinstance(in_obj, uuid.UUID):
                in_obj = ArtistID(in_obj)

            self._logger.debug(f"Looking up artist {in_obj} in Musicbrainz Database")

            stmt = sa.select(mbdata.models.Artist).where(mbdata.models.Artist.gid == str(in_obj))
            a: mbdata.models.Artist = session.scalar(stmt)
            if a is None:
                raise MBIDNotExistsError(f"No Artist with ID '{str(in_obj)}'")

            # aliases
            stmt = sa.select(mbdata.models.ArtistAlias).where(mbdata.models.ArtistAlias.artist == a)
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
                factory=self
            )

            return artist

    def get_release_group(self, in_obj: ReleaseGroupID | str | uuid.UUID) -> ReleaseGroup:
        with db.get_db_session() as session:
            if isinstance(in_obj, str) or isinstance(in_obj, uuid.UUID):
                in_obj = ReleaseGroupID(in_obj)

            self._logger.debug(f"Looking up release group {in_obj} in Musicbrainz Database")

            stmt = sa.select(mbdata.models.ReleaseGroup).where(mbdata.models.ReleaseGroup.gid == str(in_obj))
            rg: mbdata.models.ReleaseGroup = session.scalar(stmt)

            if rg is None:
                raise MBIDNotExistsError(f"No Release Group with ID '{str(in_obj)}'")

            artists = [self.get_artist(ArtistID(str(a.artist.gid))) for a in rg.artist_credit.artists]
            primary_type = ReleaseType(rg.type.name) if rg.type is not None else None
            types = ([primary_type] if primary_type is not None else []) + [ReleaseType(s.secondary_type.name) for s in
                                                                            rg.secondary_types]

            #get aliases
            stmt = sa.select(mbdata.models.ReleaseGroupAlias).where(
                mbdata.models.ReleaseGroupAlias.release_group == rg)
            rgas: list[mbdata.models.ReleaseGroupAlias] = session.scalars(stmt).all()
            aliases = [rg.name]
            for rga in rgas:
                if rga.name not in aliases:
                    aliases.append(rga.name)

            release_group = ReleaseGroup(
                id=in_obj,
                _db_id=rg.id,
                factory=self,
                title=rg.name,
                artists=artists,
                aliases=aliases,
                primary_type=primary_type,
                types=types,
                disambiguation=rg.comment,
                artist_credit_phrase=rg.artist_credit.name,
                is_va=(rg.artist_credit_id == 1)
            )

            return release_group

    def get_release(self, in_obj: ReleaseID | str | uuid.UUID) -> Release:
        with db.get_db_session() as session:
            if isinstance(in_obj, str) or isinstance(in_obj, uuid.UUID):
                in_obj = ReleaseID(in_obj)

            self._logger.debug(f"Looking up release {in_obj} in Musicbrainz Database")

            stmt = sa.select(mbdata.models.Release).where(mbdata.models.Release.gid == str(in_obj))
            rel: mbdata.models.Release = session.scalar(stmt)

            if rel is None:
                raise MBIDNotExistsError(f"No Release with ID '{str(in_obj)}'")

            artists = [self.get_artist(ArtistID(str(a.artist.gid))) for a in rel.artist_credit.artists]
            first_release_date: datetime.date = util.parse_partial_date(
                rel.first_release.date) if rel.first_release is not None else None
            countries: list[str] = [util.area_to_country_db(c.country.area) for c in rel.country_dates]

            # get aliases
            stmt = sa.select(mbdata.models.ReleaseAlias).where(
                mbdata.models.ReleaseAlias.release_id == rel.id)
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
                factory=self
            )

        return release

    def get_recording(self, in_obj: RecordingID | str | uuid.UUID) -> Recording:
        with db.get_db_session() as session:
            if isinstance(in_obj, str) or isinstance(in_obj, uuid.UUID):
                in_obj = RecordingID(in_obj)

            self._logger.debug(f"Looking up recording {in_obj} in Musicbrainz Database")

            stmt = sa.select(mbdata.models.Recording).where(mbdata.models.Recording.gid == str(in_obj))
            rec: mbdata.models.Recording = session.scalar(stmt)
            if rec is None:
                raise MBIDNotExistsError(f"No recording with id '{in_obj}'")

            first_release_date = util.parse_partial_date(
                rec.first_release.date) if rec.first_release is not None else None

            #aliases
            stmt = sa.select(mbdata.models.RecordingAlias).where(
                mbdata.models.RecordingAlias.recording == rec)
            ras: list[mbdata.models.RecordingAlias] = session.scalars(stmt).all()
            aliases = [rec.name]
            for ra in ras:
                if ra.name not in aliases:
                    aliases.append(ra.name)

            recording = Recording(
                id=RecordingID(str(rec.gid)),
                _db_id=rec.id,
                artists=[self.get_artist(ArtistID(str(a.artist.gid))) for a in rec.artist_credit.artists],
                title=rec.name,
                artist_credit_phrase=rec.artist_credit.name,
                disambiguation=rec.comment,
                first_release_date=first_release_date,
                aliases=aliases,
                factory=self
            )
        return recording

    def get_medium(self, in_obj: MediumID | str | uuid.UUID) -> Medium:
        raise NotImplementedError

    def get_track(self, in_obj: TrackID | str | uuid.UUID) -> Track:
        raise NotImplementedError

    def get_work(self, in_obj: WorkID | str | uuid.UUID) -> Work:
        with db.get_db_session() as session:
            if isinstance(in_obj, str) or isinstance(in_obj, uuid.UUID):
                in_obj = WorkID(in_obj)

            self._logger.debug(f"Looking up work {in_obj} in Musicbrainz Database")

            stmt = sa.select(mbdata.models.Work).where(mbdata.models.Work.gid == str(in_obj))
            w: mbdata.models.Work = session.scalar(stmt)

            if w is None:
                raise MBIDNotExistsError(f"No Work with ID '{str(in_obj)}'")

            work = Work(
                id=WorkID(str(w.gid)),
                _db_id=w.id,
                title=w.name,
                disambiguation=w.comment,
                type=w.type.name if w.type is not None else None,
                factory=self
            )
            return work

    def performance_of_recording(self, recording: Recording) -> tuple[list[Work], list[PerformanceWorkAttributes]]:
        with db.get_db_session() as session:
            if recording._db_id is None:
                # TODO implement: determine correct id if recording didn't come from DBFactory.
                raise NotImplementedError

            stmt = sa.select(mbdata.models.LinkRecordingWork). \
                where(mbdata.models.LinkRecordingWork.entity0_id == str(recording._db_id))
            res: list[mbdata.models.LinkRecordingWork] = session.scalars(stmt).all()
            if res is None or len(res) == 0:
                return [], []
            else:
                ws = [self.get_work(r.work.gid) for r in res]

            types = []
            for r in res:
                stmt = sa.select(mbdata.models.LinkAttribute). \
                    where(mbdata.models.LinkAttribute.link == r.link)
                res2: list[mbdata.models.LinkAttribute] = session.scalars(stmt).all()

                [types.append(PerformanceWorkAttributes(att.attribute_type.name)) for att in res2 if
                 PerformanceWorkAttributes(att.attribute_type.name) not in types]

            return ws, types


class APIFactory(MBFactory):
    _logger: logging.Logger = logging.getLogger(__name__)

    def __init__(self, backup_factory: MBFactory = None):
        super().__init__(backup_factory)
        if not musicbrainz_api.is_configured_musicbrainzngs():
            self._logger.debug(f"Musicbrainzngs library not initialized. Configuring with default values.")
            if self.backup_factory is not None:
                self._logger.debug(f"Using factory of type {type(self.backup_factory)} as backup")
            musicbrainz_api.configure_musicbrainzngs()

    def get_artist(self, in_obj: ArtistID | str | uuid.UUID) -> Artist:

        if isinstance(in_obj, str) or isinstance(in_obj, uuid.UUID):
            in_obj = ArtistID(in_obj)

        self._logger.debug(f"Looging up {in_obj} via Musicbrainz API.")

        result = musicbrainzngs.get_artist_by_id(
            id=str(in_obj),
            includes=['aliases']
        )

        artist = Artist(
            id=ArtistID(result['artist']['id']),
            _db_id=None,
            name=result['artist']['name'],
            artist_type=result['artist']['type'] if 'type' in result['artist'] else None,
            sort_name=result['artist']['sort-name'],
            disambiguation=result['artist']['disambiguation'],
            aliases=[a['alias'] for a in result['artist']['alias-list']] if 'alias-list' in result['artist'] else [],
            country=result['artist']['country'],
            factory=self
        )

        return artist

    def get_release_group(self, in_obj: ReleaseGroupID | str | uuid.UUID) -> ReleaseGroup:
        # TODO: Implement
        raise NotImplementedError

    def get_release(self, in_obj: ReleaseID | str | uuid.UUID) -> Release:
        # TODO: Implement
        raise NotImplementedError

    def get_recording(self, in_obj: RecordingID | str | uuid.UUID) -> Recording:
        # TODO: Implement
        raise NotImplementedError

    def get_medium(self, in_obj: MediumID | str | uuid.UUID) -> Medium:
        # TODO: Implement
        raise NotImplementedError

    def get_track(self, in_obj: TrackID | str | uuid.UUID) -> Track:
        # TODO: Implement
        raise NotImplementedError

    def get_work(self, in_obj: WorkID | str | uuid.UUID) -> Work:
        # TODO: Implement
        raise NotImplementedError

    def performance_of_recording(self, recording: Recording) -> tuple[list[Work], list[PerformanceWorkAttributes]]:
        # TODO: Implement
        raise NotImplementedError
