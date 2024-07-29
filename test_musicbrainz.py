import logging
import os
import pathlib

import requests
from requests.adapters import HTTPAdapter
from requests_file import FileAdapter
from urllib3 import Retry

from src.musicbrainz_wrapper import *
from src.musicbrainz_wrapper import db

if __name__ == "__main__":
    logging.basicConfig(format="%(levelname)-8s:%(asctime)s:%(name)-30s:%(lineno)-4s:%(message)s", level=logging.DEBUG)
    logging.getLogger().setLevel(logging.DEBUG)

    set_datadir(pathlib.Path(__file__).parent.absolute())

    MBApi.configure(search_cache_default=True, fetch_cache_default=True)

    db_session = db.get_db_session()


    a = Artist(ArtistID("0df890e1-f4f2-4b21-a413-cd8af1af32d8"))
    b = ReleaseGroup(ReleaseGroupID("94e8bbe7-788d-3000-8e40-57b7591d4fb4"))
    c = Release(ReleaseID("a6f67b96-5f97-495c-b224-ec93d521f922"))
    c1 = c.mediums
    c2 = c1[0].tracks
    d = Recording(RecordingID("901aa230-e85e-4305-b922-d78a9d62643f"))
    d1 = d.performance_of
    d2 = d1.performances

    d3 = d.siblings
    e = Recording(RecordingID("77601dfe-df14-4894-a8b7-c5c68ca25e11"))
    e1 = e.performance_of
    e2 = e.streams
    e3 = e.siblings

    f = Recording(RecordingID('b1a9c0e9-d987-4042-ae91-78d6a3267d69'))
    f1 = f.streams
    f2 = f.siblings
    f3 = f.spotify_id
    exit()

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

        s2 = search.search_by_recording_id(RecordingID("77601dfe-df14-4894-a8b7-c5c68ca25e11"), mb_api=mb)


        y = search.search_name("DJ Paul Elstak", "Rainbow in the Sky", mb_api=mb)

        zzzz = find_best_release_group(artist_query="DJ Paul Elstak", title_query="Rainbow in the sky", mb_api=mb)
        print(zzzz)
        zzzz2 = find_best_release_group(artist_query="DJ Paul Elstak", title_query="Rainbow in the sky", mb_api=mb, canonical=False)
        print(zzzz2)
        pass
