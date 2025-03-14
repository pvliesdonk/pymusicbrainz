import urllib3

DEFAULT_APP: str = "My Tagger"
DEFAULT_VERSION: str = "0.1"
DEFAULT_CONTACT: str = "https://music.liesdonk.nl"
DEFAULT_API_URL: str = "musicbrainz.org"
DEFAULT_HTTPS: bool = True
DEFAULT_RATE_LIMIT: bool = True
DEFAULT_DB_URI: str = 'postgresql://musicbrainz:musicbrainz@hades.int.liesdonk.nl/musicbrainz_db'
DEFAULT_TYPESENSE_URL: urllib3.util.Url = urllib3.util.parse_url("http://hades.int.liesdonk.nl:8108")
DEFAULT_TYPESENSE_API_KEY: str = "xyz"
DEFAULT_TYPESENSE_SEARCH_FIELD: str = "combined"
DEFAULT_TYPESENSE_COLLECTION: str = "musicbrainz"

ACOUSTID_APIKEY = "7z40OrGgVS"
