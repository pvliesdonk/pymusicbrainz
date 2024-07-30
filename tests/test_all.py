import logging

from context import musicbrainz
from context.musicbrainz.datatypes import *

DB_URI: str = "postgresql://musicbrainz:musicbrainz@musicbrainz.int.liesdonk.nl/musicbrainz_db"

logging.basicConfig(format="%(levelname)-8s:%(asctime)s:%(name)-30s:%(lineno)-4s:%(message)s", level=logging.DEBUG)
logging.getLogger().setLevel(logging.DEBUG)

logging.info(f"Configuring database at {DB_URI}")
musicbrainz.configure_database(db_url=DB_URI)

musicbrainz.configure_musicbrainzngs()
musicbrainz.configure_typesense()


a = musicbrainz.get_artist(ArtistID("026c4d7c-8dfe-46e8-ab14-cf9304d6863d"))
b = musicbrainz.get_release_group(ReleaseGroupID("94e8bbe7-788d-3000-8e40-57b7591d4fb4"))
c = musicbrainz.get_release(ReleaseID("a6f67b96-5f97-495c-b224-ec93d521f922"))
d = musicbrainz.get_recording(RecordingID("77601dfe-df14-4894-a8b7-c5c68ca25e11"))
d1 = d.performance_of
d2 = d.siblings

e = musicbrainz.get_work(WorkID("3705e2ef-c3d4-3683-9bd7-8574d1749a8d"))
e1 = e.performances

f = musicbrainz.search_artist_musicbrainz("DJ Paul Elstak")

g = musicbrainz.search_song_canonical("DJ Paul Elstak", "Rainbow in the Sky")

h = musicbrainz.search_song_musicbrainz("DJ Paul Elstak", "Rainbow in the Sky")

i = musicbrainz.search_by_recording_id(RecordingID("77601dfe-df14-4894-a8b7-c5c68ca25e11"))

j = musicbrainz.search_song("DJ Paul Elstak", "Rainbow in the Sky")

zzzz = find_best_release_group(artist_query="DJ Paul Elstak", title_query="Rainbow in the sky")
print(zzzz)
zzzz2 = find_best_release_group(artist_query="DJ Paul Elstak", title_query="Rainbow in the sky", canonical=False)
print(zzzz2)
pass

