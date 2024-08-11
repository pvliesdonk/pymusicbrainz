import logging
import pathlib

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


fkdjhaksdfj = pymusicbrainz.search_song("The Bangles", "A Hazy Shade of Winter")

gsfhgdsfg = pymusicbrainz.search_song("4 Tops","Reach Out I'll Be There")
dsaafs = pymusicbrainz.search_song('3T (featuring Herbie)', 'Gotta Be You')

yyy = pymusicbrainz.get_artist("5441c29d-3602-4898-b1a1-b77fa23b8e50")
yyy.country

# xx = pymusicbrainz.get_recording("3cee1d9e-49ce-448e-b261-6b595c84861e")
#
# xx.performance_of
# xx.performance_type
# xx2 = xx.siblings
path = pathlib.Path('tests')/'test_file.flac'
if path.exists():
    dsakj = pymusicbrainz.search_fingerprint(path)
    dsakj_result = dsakj.get_best_result()

aa = pymusicbrainz.search_song( 'Queen', 'Bohemian Rhapsody' )
aa.get_best_result()

a = pymusicbrainz.get_artist("019cdf80-f8fe-4b2e-ad34-a84285427848")
a.country

c = pymusicbrainz.get_release("ffefaec6-3cec-4252-ab9d-12d96543d4cf")
c.countries

xxx = pymusicbrainz.get_work("1d2ab3b6-22e3-347c-98a3-fca099ffc910")

Z = pymusicbrainz.search_song('Britney Spears', 'Born To Make You Happy')
Z_result = Z.get_best_result()

A = pymusicbrainz.search_song('The Beatles', 'I Feel Fine')
A_result = A.get_best_result()

B = pymusicbrainz.search_song('Marco', 'Binnen')
B_result = B.get_best_result()

C =pymusicbrainz.search_song("DJ Paul Elstak", "Rainbow in the Sky")
C_result = C.get_best_result()

a = pymusicbrainz.get_artist("026c4d7c-8dfe-46e8-ab14-cf9304d6863d")
a1 = a.release_group_ids
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
j_result = j.get_best_result()
#
# zzzz = find_best_release_group(artist_query="DJ Paul Elstak", title_query="Rainbow in the sky")
# print(zzzz)
# zzzz2 = find_best_release_group(artist_query="DJ Paul Elstak", title_query="Rainbow in the sky", canonical=False)
# print(zzzz2)
pass

