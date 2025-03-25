from __future__ import annotations

import logging
import pathlib
import sqlite3

import urllib3

from pymusicbrainz import config
from pymusicbrainz.exceptions import CanonicalDBNotAvailable, NoCanonicalFound
from pymusicbrainz.identifiers import RecordingID, ReleaseID, ReleaseGroupID
from pymusicbrainz.mbdataclass import Recording, Release


class CanonicalSearch:

    _instance: CanonicalSearch = None
    _logger: logging.Logger = logging.getLogger(__name__)

    @classmethod
    def get_instance(
        cls,
        db_file: pathlib.Path = config.DEFAULT_CANONICAL_DB,
        url: urllib3.util.Url = config.DEFAULT_TYPESENSE_URL,
        api_key: str = config.DEFAULT_TYPESENSE_API_KEY,
        collection: str = config.DEFAULT_TYPESENSE_SEARCH_FIELD,
        search_field: str = config.DEFAULT_TYPESENSE_COLLECTION,
    ) -> CanonicalSearch:
        if cls._instance is None:
            cls._instance = CanonicalSearch(
                db_file=db_file,
                url=url,
                api_key=api_key,
                collection=collection,
                search_field=search_field,
            )

        return cls._instance

    def __init__(
        self,
        db_file: pathlib.Path = config.DEFAULT_CANONICAL_DB,
        url: urllib3.util.Url = config.DEFAULT_TYPESENSE_URL,
        api_key: str = config.DEFAULT_TYPESENSE_API_KEY,
        collection: str = config.DEFAULT_TYPESENSE_SEARCH_FIELD,
        search_field: str = config.DEFAULT_TYPESENSE_COLLECTION,
    ):
        if db_file:
            try:
                self._canonical_db: sqlite3.Connection = sqlite3.connect(db_file)
            except:
                self._canonical_db = None
        else:
            self._canonical_db = None

        self._typesense_url: urllib3.util.Url = url
        self._typesense_api_key: str = api_key
        self._typesense_collection: str = collection
        self._typesense_search_field: str = search_field

        pass

    def is_available_canonical(self) -> bool:
        return self._canonical_db is not None

    def get_canonical_recording(
        self, recording: RecordingID | Recording
    ) -> tuple[RecordingID, ReleaseID]:
        if not self.is_available_canonical():
            raise CanonicalDBNotAvailable()

        if isinstance(recording, Recording):
            mbid = recording.id
        else:
            mbid = recording

        stmt = """
        select canonical_recording_mbid, canonical_release_mbid 
        from canonical_recording_redirect 
        where canonical_recording_redirect.recording_mbid = :mbid
        """
        self._canonical_db: sqlite3.Connection
        cur = self._canonical_db.execute(stmt, {"mbid": str(mbid)})
        res = cur.fetchone()
        if res is None:
            raise NoCanonicalFound()
        return RecordingID(res[0]), ReleaseID(res[1])

    def get_canonical_release(
        self, release: ReleaseID | Release
    ) -> tuple[ReleaseID, ReleaseGroupID]:
        if not self.is_available_canonical():
            raise CanonicalDBNotAvailable()

        if isinstance(release, Release):
            mbid = release.id
        else:
            mbid = release

        stmt = """
        select canonical_release_mbid, release_group_mbid 
        from canonical_release_redirect 
        where canonical_release_redirect.release_mbid = :mbid
        """
        self._canonical_db: sqlite3.Connection
        cur = self._canonical_db.execute(stmt, {"mbid": str(mbid)})
        res = cur.fetchone()
        if res is None:
            raise NoCanonicalFound()
        return ReleaseID(res[0]), ReleaseGroupID(res[1])

    #
    # def _search_typesense(self, artist_name, recording_name):
    #     hits = do_typesense_lookup(artist_name, recording_name)
    #
    #     output = []
    #     for hit in hits:
    #         try:
    #             hit["artists"] = [
    #                 get_artist(artist_redirect(x)) for x in hit["artist_ids"]
    #             ]
    #             hit["release"] = get_release(release_redirect(hit["release_id"]))
    #             hit["recording"] = get_recording(
    #                 recording_redirect(hit["recording_id"])
    #             )
    #             hit["release_group"] = hit["release"].release_group
    #             output.append(hit)
    #         except MBApiError as ex:
    #             _logger.error(f"Could not process hit from typesense response")
    #     return output


if __name__ == "__main__":
    cs = CanonicalSearch.get_instance(
        db_file=pathlib.Path("../data/canonical_redirect.db")
    )
    rid = RecordingID("f2ff45c9-e6d8-4d18-885e-0e1e0a0e30ea")
    a, b = cs.get_canonical_recording(rid)

    pass
