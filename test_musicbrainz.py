import logging

from src.musicbrainz_wrapper import *
from src.musicbrainz_wrapper import canonical

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.DEBUG)

    MBApi.configure(search_cache_default=True, fetch_cache_default=True)

    sess = canonical.get_session()

    mb: MBApi
    with MBApi() as mb:
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
        y = mb.search_recording("DJ Paul Elstak", "Rainbow in the Sky")
        pass