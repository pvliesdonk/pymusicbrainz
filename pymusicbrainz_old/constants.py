import urllib3

from .datatypes import ArtistID, ReleaseType

DEFAULT_APP: str = "My Tagger"
DEFAULT_VERSION: str = "0.1"
DEFAULT_CONTACT: str = "https://music.liesdonk.nl"
DEFAULT_API_URL: str = "musicbrainz.org"
DEFAULT_HTTPS: bool = True
DEFAULT_RATE_LIMIT: bool = True
DEFAULT_DB_URI: str = 'postgresql://musicbrainz:musicbrainz@127.0.0.1/musicbrainz'
DEFAULT_TYPESENSE_URL: urllib3.util.Url = urllib3.util.parse_url("http://musicbrainz.int.liesdonk.nl:8108")
DEFAULT_TYPESENSE_API_KEY: str = "xyz"
DEFAULT_TYPESENSE_SEARCH_FIELD: str = "combined"
DEFAULT_TYPESENSE_COLLECTION: str = "musicbrainz"

ACOUSTID_APIKEY = "7z40OrGgVS"

PRIMARY_TYPES = {
    ReleaseType.ALBUM: 1,
    ReleaseType.SINGLE: 2,
    ReleaseType.EP: 3,
    ReleaseType.OTHER: 11,
    ReleaseType.BROADCAST: 12}
SECONDARY_TYPES = {
    ReleaseType.COMPILATION: 1,
    ReleaseType.SOUNDTRACK: 2,
    ReleaseType.SPOKENWORD: 3,
    ReleaseType.INTERVIEW: 4,
    ReleaseType.AUDIOBOOK: 5,
    ReleaseType.LIVE: 6,
    ReleaseType.REMIX: 7,
    ReleaseType.DJ_MIX: 8,
    ReleaseType.MIXTAPE: 9,
    ReleaseType.DEMO: 10,
    ReleaseType.AUDIODRAMA: 11,
    ReleaseType.FIELDRECORDING: 12
}
UNKNOWN_ARTIST_ID = ArtistID("125ec42a-7229-4250-afc5-e057484327fe")
VA_ARTIST_ID = ArtistID("89ad4ac3-39f7-470e-963a-56509c546377")

INT_COUNTRIES = ["XW", "XE"]
FAVORITE_COUNTRIES = ["NL", "GB", "US"] + INT_COUNTRIES

ACOUSTID_META = [ "recordings", "recordingids", "releases", "releaseids", "releasegroups", "releasegroupids", "tracks", "compress", "usermeta", "sources"]
