import logging
import pathlib


from context import pymusicbrainz
from pymusicbrainz import MusicbrainzSearchResult
from pymusicbrainz.datatypes import ReleaseType

DB_URI: str = "postgresql://musicbrainz:musicbrainz@musicbrainz.int.liesdonk.nl/musicbrainz_db"

logging.basicConfig(format="%(levelname)-8s:%(asctime)s:%(name)-30s:%(lineno)-4s:%(message)s", level=logging.DEBUG)
logging.getLogger().setLevel(logging.DEBUG)
logging.getLogger('musicbrainzngs').setLevel(logging.ERROR)
logging.getLogger('urllib3').setLevel(logging.ERROR)

logging.info(f"Configuring database at {DB_URI}")
pymusicbrainz.configure_database(db_url=DB_URI)


pymusicbrainz.configure_musicbrainzngs()
pymusicbrainz.configure_typesense()
#pymusicbrainz.configure_object_cache(pathlib.Path("object_cache.db"))

pymusicbrainz.configure_hintfile(pathlib.Path("hints.json"))
pymusicbrainz.add_artist_name_hint("AEDM", "Acda en de Munnik")
pymusicbrainz.add_title_name_hint("Test_title", "test title")
pymusicbrainz.add_recording_name_hint("alpha", "beta", "gamma", "delta")
pymusicbrainz.add_recording_id_hint(match_artist="flopdwork", match_title="vlobbert", recording_id=pymusicbrainz.RecordingID("2bb74cf7-acd5-4f7b-9be1-1c9eceb96a3d"))


rg = pymusicbrainz.get_release_group("810068af-2b3c-3e9c-b2ab-68a3f3e3787d")
urls = rg.external_urls
did = rg.discogs_ids

a = pymusicbrainz.get_recording(pymusicbrainz.RecordingID("1890677b-f00e-4df8-b317-9e58a81ca9e2"))


a = pymusicbrainz.get_artist("0383dadf-2a4e-4d10-a46a-e9e041da8eb3")
aa = a.release_groups

sdjhskdfjh = pymusicbrainz.search_song('Allman Brothers Band', 'Jessica')
b = pymusicbrainz.get_release("18db7ddc-b41e-4e84-8506-6ee56ac2e77d")
ccc = b.mediums
flshkjds_year = pymusicbrainz.search_song('Queen', 'Bohemian Rhapsody', year=1975)

pymusicbrainz.save_hints()

wjefefhiuh_fast = pymusicbrainz.search_song('The Clash', 'Should I Stay Or Should I Go', attempt_fast=True)

a = pymusicbrainz.get_recording(pymusicbrainz.RecordingID("1890677b-f00e-4df8-b317-9e58a81ca9e2"))
b = pymusicbrainz.get_recording(pymusicbrainz.RecordingID("c04a6c24-f4c5-4dfa-8b6e-359fd6c1db30"))
c = a.is_years_older_than(b)


seed = pymusicbrainz.RecordingID("2bb74cf7-acd5-4f7b-9be1-1c9eceb96a3d")
seed2 = pymusicbrainz.RecordingID("2bb74cf7-acd5-4f7b-9be1-1c9eceb96a3d")
eq = (seed == seed2)

rec = pymusicbrainz.get_recording(pymusicbrainz.RecordingID("49d5edae4f854b40bb3030b4f71dbbae"))
rgs = rec.release_groups
album = rec.studio_albums
album2 = rec.live_albums
res = MusicbrainzSearchResult.result_from_recording(rec)

boneym = pymusicbrainz.search_song("Boney M", "Sunny")


hint = pymusicbrainz.find_hint_recording("Flopdwork","Vlobbert")
fsdlkjls = pymusicbrainz.search_song("Flopdwork","Vlobbert")
askjdas = fsdlkjls.get_best_result()

lsdkjfsdlk = pymusicbrainz.search_song("Status Quo",	"Roll Over Lay Down (live)")


sfdgdsfg = pymusicbrainz.search_song(seed_id=pymusicbrainz.RecordingID("2bb74cf7-acd5-4f7b-9be1-1c9eceb96a3d"))

wjefefhiuh = pymusicbrainz.search_song('Justin Timberlake', 'CAN’T STOP THE FEELING!')

fdjoiaj = pymusicbrainz.search_song('David Glen Eisley', 'Sweet Victory')

sdfdsk = pymusicbrainz.title_is_live("Trapped (Live)")
fdsj = pymusicbrainz.search_song_musicbrainz('Bruce Springsteen', 'Trapped', secondary_type=[ReleaseType.LIVE])
sdhksfdjh = pymusicbrainz.search_song('Bruce Springsteen', 'Trapped (Live)')

str1 = pymusicbrainz.get_object_from_id(pymusicbrainz.util.id_from_string("5441c29d-3602-4898-b1a1-b77fa23b8e50"))
str2 = pymusicbrainz.util.id_from_string("3cee1d9e-49ce-448e-b261-6b595c84861e")
str3 = pymusicbrainz.util.id_from_string('https://musicbrainz.org/recording/0bb24ca8-0268-4649-83f4-40c5d9219be5')


sdffsdk = pymusicbrainz.search_song(seed_id=pymusicbrainz.util.id_from_string("c9084c90-ccc1-42f8-a1da-e9345455908c"))

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
    dsakj = pymusicbrainz.search_song("ZZ Top","Gimme All Your Lovin'",file=path)
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

