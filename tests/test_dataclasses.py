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
rel_id = pymusicbrainz.ReleaseID("a6f67b96-5f97-495c-b224-ec93d521f922")
track_id = pymusicbrainz.TrackID("ed3cb340-c11b-3580-a646-eaba1d109edd")
rec_id = pymusicbrainz.RecordingID("77601dfe-df14-4894-a8b7-c5c68ca25e11")
work_id = pymusicbrainz.WorkID("3705e2ef-c3d4-3683-9bd7-8574d1749a8d")

# database lookup

db_factory = pymusicbrainz.MBFactory.get_factory() #pymusicbrainz.factory.DBFactory()


artist_db = db_factory.get_artist(a_id)
print(artist_db)

release_group_db = db_factory.get_release_group(rg_id)
print(release_group_db)

release_db = db_factory.get_release(rel_id)
print(release_db)

recording_db = db_factory.get_recording(rec_id)
print(recording_db)

track_db = db_factory.get_track(track_id)
print(track_db)

work_db = db_factory.get_work(work_id)
print(work_db)

# API lookup


api_factory = pymusicbrainz.factory.APIFactory()


artist_api = api_factory.get_artist(a_id)
print(artist_api)

release_group_api = api_factory.get_release_group(rg_id)
print(release_group_api)

release_api = api_factory.get_release(rel_id)
print(release_api)

recording_api = api_factory.get_recording(rec_id)
print(recording_api)

track_api = api_factory.get_track(track_id)
print(track_api)

work_api = api_factory.get_work(work_id)
print(work_api)


pass