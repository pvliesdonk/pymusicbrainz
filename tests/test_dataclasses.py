from __future__ import annotations

import logging
import pprint

from context import pymusicbrainz

logging.basicConfig(
    format="%(levelname)-8s:%(asctime)s:%(name)-30s:%(lineno)-4s:%(message)s",
    level=logging.DEBUG,
)
logging.getLogger().setLevel(logging.DEBUG)
logging.getLogger("musicbrainzngs").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)


a_id = pymusicbrainz.ArtistID("0383dadf-2a4e-4d10-a46a-e9e041da8eb3")
rg_id = pymusicbrainz.ReleaseGroupID("120c786d-a3b2-3c19-b4ff-2b7b3b4435bf")
rel_id = pymusicbrainz.ReleaseID("a6f67b96-5f97-495c-b224-ec93d521f922")
track_id = pymusicbrainz.TrackID("ed3cb340-c11b-3580-a646-eaba1d109edd")
rec_id = pymusicbrainz.RecordingID("776e8086-0033-4cbc-a60f-407588152b4d")
work_id = pymusicbrainz.WorkID("3705e2ef-c3d4-3683-9bd7-8574d1749a8d")

# database lookup

db_factory = pymusicbrainz.MBFactory.get_factory()  # pymusicbrainz.factory.DBFactory()


artist_db = db_factory.get_artist(a_id)
print(artist_db)
# artist_rgs_db = list(artist_db.get_release_groups())


artist_soundtracks_db = list(artist_db.get_soundtracks())
for rg in artist_soundtracks_db:
    print(rg)

release_group_db = db_factory.get_release_group(rg_id)
print(release_group_db)

release_db = db_factory.get_release(rel_id)
print(release_db)

recording_db = db_factory.get_recording(rec_id)
print(recording_db)

work_db = db_factory.get_work(work_id)
print(work_db)
# p = work_db.performances


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

work_api = api_factory.get_work(work_id)
print(work_api)


pass
