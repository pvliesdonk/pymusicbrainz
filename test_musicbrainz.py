import logging
import os
import pathlib

import requests
from requests.adapters import HTTPAdapter
from requests_file import FileAdapter
from urllib3 import Retry

from src.musicbrainz_wrapper import *
from src.musicbrainz_wrapper import canonical
from src.musicbrainz_wrapper.search import SearchType


from musicbrainz_wrapper import typesense_api

if __name__ == "__main__":
    logging.basicConfig(format="%(levelname)-8s:%(asctime)s:%(name)-30s:%(lineno)-4s:%(message)s", level=logging.DEBUG)
    logging.getLogger().setLevel(logging.DEBUG)

    set_datadir(pathlib.Path(__file__).parent.absolute())

    MBApi.configure(search_cache_default=True, fetch_cache_default=True)

    db_session = canonical.get_session()

    req_session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=0.1,
        status_forcelist=[502, 503, 504],
        allowed_methods={'POST'},
    )
    req_session.mount('https://', HTTPAdapter(max_retries=retries))
    req_session.mount('file://', FileAdapter())

    url = os.environ.get('MB_CANONICAL_DUMP_URL', canonical.get_canonical_dump_url())


    # canonical.get_canonical_dump(url = url, req_session=req_session, db_session=db_session)


    mb: MBApi
    with MBApi() as mb:
        hits = mb.typesense_lookup("DJ Paul Elstak", "Rainbow in the sky")

        mb.disable_mirror()
        a = mb.get_artist_by_id(ArtistID("026c4d7c-8dfe-46e8-ab14-cf9304d6863d"))
        b = mb.get_release_group_by_id(ReleaseGroupID("94e8bbe7-788d-3000-8e40-57b7591d4fb4"))
        c = mb.get_release_by_id(ReleaseID("a6f67b96-5f97-495c-b224-ec93d521f922"))
        d = mb.get_recording_by_id(RecordingID("77601dfe-df14-4894-a8b7-c5c68ca25e11"))
        yy = d.performance_of_id
        xx = d.siblings

        e = mb.get_work_by_id(WorkID("3705e2ef-c3d4-3683-9bd7-8574d1749a8d"))
        zz = e.performance_ids

        x = mb.search_artists("DJ Paul Elstak")

        a1 = search.search_canonical_release("DJ Paul Elstak", "Rainbow in the Sky", mb_api=mb)

        a2 = search.search_studio_albums_by_recording_ids(RecordingID("77601dfe-df14-4894-a8b7-c5c68ca25e11"), mb_api=mb)
        print(a2)
        a3 = search.search_soundtracks_by_recording_ids(RecordingID("77601dfe-df14-4894-a8b7-c5c68ca25e11"), mb_api=mb)
        print(a3)
        a4 = search.search_singles_by_recording_ids(RecordingID("77601dfe-df14-4894-a8b7-c5c68ca25e11"), mb_api=mb)
        print(a4)
        a5 = search.search_eps_by_recording_ids(RecordingID("77601dfe-df14-4894-a8b7-c5c68ca25e11"), mb_api=mb)
        print(a5)
        a6 = search.search_release_groups_by_recording_ids(RecordingID("77601dfe-df14-4894-a8b7-c5c68ca25e11"), mb_api=mb)
        print(a6)


        y = mb.search_recording("DJ Paul Elstak", "Rainbow in the Sky")

        zzzz = find_best_release_group(artist_query="DJ Paul Elstak", title_query="Rainbow in the sky", mb_api=mb)
        print(zzzz)
        zzzz2 = find_best_release_group(artist_query="DJ Paul Elstak", title_query="Rainbow in the sky", mb_api=mb, canonical=False)
        print(zzzz2)
        pass
