from __future__ import annotations

import logging
import uuid
from abc import ABC, abstractmethod

import musicbrainzngs
import sqlalchemy as sa
import mbdata.models

from pymusicbrainz import identifiers, mbdataclass, db, exceptions, util, musicbrainz_api, musicbrainz_types


class MBFactory(ABC):

    @staticmethod
    def get_factory() -> MBFactory:
        try:
            factory = DBFactory()
            logging.getLogger(__name__).debug("Instantiated a DB Factory")
        except exceptions.FactoryNotAvailable as ex:
            logging.getLogger(__name__).debug("Database not available. Instantiated an APIFactory")
            factory = APIFactory()

        return factory

    @abstractmethod
    def get_artist(self, in_obj: identifiers.ArtistID | str | uuid.UUID) -> mbdataclass.Artist:
        pass

    @abstractmethod
    def get_release_group(self, in_obj: identifiers.ReleaseGroupID | str | uuid.UUID) -> mbdataclass.ReleaseGroup:
        pass

    @abstractmethod
    def get_release(self, in_obj: identifiers.ReleaseID | str | uuid.UUID) -> mbdataclass.Release:
        pass

    @abstractmethod
    def get_recording(self, in_obj: identifiers.RecordingID | str | uuid.UUID) -> mbdataclass.Recording:
        pass

    @abstractmethod
    def get_medium(self, in_obj: identifiers.MediumID | str | uuid.UUID) -> mbdataclass.Medium:
        pass

    @abstractmethod
    def get_track(self, in_obj: identifiers.TrackID | str | uuid.UUID) -> mbdataclass.Track:
        pass

    @abstractmethod
    def get_work(self, in_obj: identifiers.WorkID | str | uuid.UUID) -> mbdataclass.Work:
        pass


class DBFactory(MBFactory):
    _logger: logging.Logger = logging.getLogger(__name__)

    def __init__(self):
        # TODO: check whether database is reachable
        if False: # database not reachable
            raise exceptions.FactoryNotAvailable()
        pass

    def get_artist(self, in_obj: identifiers.ArtistID | str | uuid.UUID) -> mbdataclass.Artist:
        with db.get_db_session() as session:
            if isinstance(in_obj, str) or isinstance(in_obj, uuid.UUID):
                in_obj = identifiers.ArtistID(in_obj)

            self._logger.debug(f"Looking up {in_obj} in Musicbrainz Database")

            stmt = sa.select(mbdata.models.Artist).where(mbdata.models.Artist.gid == str(in_obj))
            a: mbdata.models.Artist = session.scalar(stmt)
            if a is None:
                raise exceptions.MBIDNotExistsError(f"No Artist with ID '{str(in_obj)}'")

            # aliases
            stmt = sa.select(mbdata.models.ArtistAlias).where(mbdata.models.ArtistAlias.artist == a)
            result = session.scalars(stmt)
            aliases = [alias.name for alias in result]

            country = util.area_to_country(a.area)

            artist = mbdataclass.Artist(
                id=identifiers.ArtistID(str(a.gid)),
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

    def get_release_group(self, in_obj: identifiers.ReleaseGroupID | str | uuid.UUID) -> mbdataclass.ReleaseGroup:
        raise NotImplementedError

    def get_release(self, in_obj: identifiers.ReleaseID | str | uuid.UUID) -> mbdataclass.Release:
        raise NotImplementedError

    def get_recording(self, in_obj: identifiers.RecordingID | str | uuid.UUID) -> mbdataclass.Recording:
        raise NotImplementedError

    def get_medium(self, in_obj: identifiers.MediumID | str | uuid.UUID) -> mbdataclass.Medium:
        raise NotImplementedError

    def get_track(self, in_obj: identifiers.TrackID | str | uuid.UUID) -> mbdataclass.Track:
        raise NotImplementedError

    def get_work(self, in_obj: identifiers.WorkID | str | uuid.UUID) -> mbdataclass.Work:
        raise NotImplementedError


class APIFactory(MBFactory):
    _logger: logging.Logger = logging.getLogger(__name__)

    def __init__(self):
        if not musicbrainz_api.is_configured_musicbrainzngs():
            self._logger.debug(f"Musicbrainzngs library not initialized. Configuring with default values.")
            musicbrainz_api.configure_musicbrainzngs()

    def get_artist(self, in_obj: identifiers.ArtistID | str | uuid.UUID) -> mbdataclass.Artist:

        if isinstance(in_obj, str) or isinstance(in_obj, uuid.UUID):
            in_obj = identifiers.ArtistID(in_obj)

        self._logger.debug(f"Looging up {in_obj} via Musicbrainz API.")

        result = musicbrainzngs.get_artist_by_id(
            id=str(in_obj),
            includes=['aliases']
        )

        artist = mbdataclass.Artist(
            id=identifiers.ArtistID(result['artist']['id']),
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

    def get_release_group(self, in_obj: identifiers.ReleaseGroupID | str | uuid.UUID) -> mbdataclass.ReleaseGroup:
        raise NotImplementedError

    def get_release(self, in_obj: identifiers.ReleaseID | str | uuid.UUID) -> mbdataclass.Release:
        raise NotImplementedError

    def get_recording(self, in_obj: identifiers.RecordingID | str | uuid.UUID) -> mbdataclass.Recording:
        raise NotImplementedError

    def get_medium(self, in_obj: identifiers.MediumID | str | uuid.UUID) -> mbdataclass.Medium:
        raise NotImplementedError

    def get_track(self, in_obj: identifiers.TrackID | str | uuid.UUID) -> mbdataclass.Track:
        raise NotImplementedError

    def get_work(self, in_obj: identifiers.WorkID | str | uuid.UUID) -> mbdataclass.Work:
        raise NotImplementedError
