import logging

from context import pymusicbrainz

DB_URI: str = "postgresql://musicbrainz:musicbrainz@musicbrainz.int.liesdonk.nl/musicbrainz_db"

logging.basicConfig(format="%(levelname)-8s:%(asctime)s:%(name)-30s:%(lineno)-4s:%(message)s", level=logging.DEBUG)
logging.getLogger().setLevel(logging.DEBUG)
logging.getLogger('musicbrainzngs').setLevel(logging.ERROR)
logging.getLogger('urllib3').setLevel(logging.ERROR)

logging.info(f"Configuring database at {DB_URI}")
pymusicbrainz.configure_database(db_url=DB_URI)

pymusicbrainz.configure_musicbrainzngs()
pymusicbrainz.configure_typesense()

A = pymusicbrainz.search_song('The Beatles', 'I Feel Fine')

a = pymusicbrainz.get_artist("026c4d7c-8dfe-46e8-ab14-cf9304d6863d")
b = pymusicbrainz.get_release_group("94e8bbe7-788d-3000-8e40-57b7591d4fb4")
c = pymusicbrainz.get_release("a6f67b96-5f97-495c-b224-ec93d521f922")
d = pymusicbrainz.get_recording("77601dfe-df14-4894-a8b7-c5c68ca25e11")
d1 = d.performance_of
d2 = d.siblings

dd = pymusicbrainz.get_recording("5119b360-2055-4f97-a795-f633df01031e")
dd1 = dd.performance_of
dd2 = dd.siblings

e = pymusicbrainz.get_work("3705e2ef-c3d4-3683-9bd7-8574d1749a8d")
e1 = e.performances

f = pymusicbrainz.search_artist_musicbrainz("DJ Paul Elstak")

g = pymusicbrainz.search_song_canonical("DJ Paul Elstak", "Rainbow in the Sky")

h = pymusicbrainz.search_song_musicbrainz("DJ Paul Elstak", "Rainbow in the Sky")

i = pymusicbrainz.search_by_recording_id(pymusicbrainz.RecordingID("77601dfe-df14-4894-a8b7-c5c68ca25e11"))

j = pymusicbrainz.search_song("DJ Paul Elstak", "Rainbow in the Sky")
#
# zzzz = find_best_release_group(artist_query="DJ Paul Elstak", title_query="Rainbow in the sky")
# print(zzzz)
# zzzz2 = find_best_release_group(artist_query="DJ Paul Elstak", title_query="Rainbow in the sky", canonical=False)
# print(zzzz2)
pass

