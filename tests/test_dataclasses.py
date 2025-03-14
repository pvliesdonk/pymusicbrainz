from __future__ import annotations

import logging
import pathlib

from context import pymusicbrainz

logging.basicConfig(format="%(levelname)-8s:%(asctime)s:%(name)-30s:%(lineno)-4s:%(message)s", level=logging.DEBUG)
logging.getLogger().setLevel(logging.DEBUG)
logging.getLogger('musicbrainzngs').setLevel(logging.ERROR)
logging.getLogger('urllib3').setLevel(logging.ERROR)


a_id = pymusicbrainz.ArtistID("026c4d7c-8dfe-46e8-ab14-cf9304d6863d")
rg_id = pymusicbrainz.ReleaseGroupID("94e8bbe7-788d-3000-8e40-57b7591d4fb4")


artist = pymusicbrainz.Artist(
    id=a_id,
    factory=None,
    name="Test_name",
    artist_type="Type",
    sort_name="Name_test",
    disambiguation="Die andere",
    aliases=["henk"],
    country="nl"
)

db_factory = pymusicbrainz.factory.DBFactory()
artist_db = db_factory.get_artist(a_id)
print(artist_db)

release_group_db = db_factory.get_release_group(rg_id)
print(release_group_db)

api_factory = pymusicbrainz.factory.APIFactory()
artist_api = api_factory.get_artist(a_id)
print(artist_api)

release_group_api = api_factory.get_release_group(rg_id)
print(release_group_api)


pass