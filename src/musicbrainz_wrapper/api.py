import datetime
import logging
import pathlib
import shelve
from contextlib import contextmanager

import musicbrainzngs

from .datatypes import ArtistID, ReleaseGroupID, ReleaseID, RecordingID, WorkID, ReleaseType, VA_ARTIST_ID, \
    UNKNOWN_ARTIST_ID, ReleaseStatus
from .exceptions import NotConfiguredError, MBApiError
from .util import split_artist
from .typesense_api import typesense_lookup

_logger = logging.getLogger(__name__)

_DEFAULT_APP: str = "My Tagger"
_DEFAULT_VERSION: str = "0.1"
_DEFAULT_CONTACT: str = "https://music.liesdonk.nl"

_DEFAULT_API_URL: str = "musicbrainz.org"
_DEFAULT_HTTPS: bool = True
_DEFAULT_RATE_LIMIT: bool = True

_DEFAULT_DB_URI: str = 'postgresql://musicbrainz:musicbrainz@127.0.0.1/musicbrainz'

ACOUSTID_APIKEY = "7z40OrGgVS"

_datadir: pathlib.Path = pathlib.Path.home() / '.mb_data'


def set_datadir(p: pathlib.Path):
    global _datadir
    _datadir = p.resolve()
    _datadir.mkdir(parents=True, exist_ok=True)
    _logger.info(f"Setting datadir to {_datadir}")


def get_datadir() -> pathlib.Path:
    if not _datadir.is_dir():
        _datadir.mkdir(parents=True, exist_ok=True)
    return _datadir


class MBApi:
    _config: dict
    _configured: bool = False
    _mb_shelf: shelve.Shelf

    _mirror_disabled: bool = False

    _object_cache: dict

    @classmethod
    def configure(cls,
                  app: str = _DEFAULT_APP,
                  version: str = _DEFAULT_VERSION,
                  contact: str = _DEFAULT_CONTACT,
                  api_url: str = _DEFAULT_API_URL,
                  use_https: bool = _DEFAULT_HTTPS,
                  rate_limit: bool = _DEFAULT_RATE_LIMIT,
                  search_cache_default: bool = True,
                  fetch_cache_default: bool = True
                  ):
        cache_file = get_datadir() / 'mb_cache'

        cls._config = {"app": app, "version": version, "contact": contact, "api_url": api_url, "use_https": use_https,
                       "rate_limit": rate_limit, "cache_file": cache_file, "search_cache_default": search_cache_default,
                       "fetch_cache_default": fetch_cache_default}

        _logger.debug(
            f"Configuring MusicBrainz API access via 'http{'s' if use_https else ''}://{api_url}' with rate limiting {'enabled' if rate_limit else 'disabled'}.")
        musicbrainzngs.set_hostname(api_url, use_https=use_https)
        musicbrainzngs.set_rate_limit(rate_limit)

        musicbrainzngs.set_useragent(app=app, version=version, contact=contact)

        _logger.debug(f"Using MusicBrainz cache in {cache_file}")
        cls._mb_shelf = shelve.open(str(cache_file))
        cls._object_cache = {}

        cls._configured = True

    def clear_cache(self):
        _logger.debug("Clearing object cache")
        self._object_cache.clear()

    def __init__(self,
                 search_cache: bool = None,
                 fetch_cache: bool = None,
                 no_mirror: bool = False):

        if not self._configured:
            raise NotConfiguredError("Please run configure() first")

        self._search_cache = search_cache if search_cache is not None else self._config["search_cache_default"]
        self._fetch_cache = fetch_cache if fetch_cache is not None else self._config["fetch_cache_default"]
        self._no_mirror = no_mirror

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.clear_cache()

    def check_mirror(self):
        if self._mirror_disabled:  # mirror disabled
            if not self._no_mirror:  # but we want the mirror
                self.enable_mirror()
        else:  # mirror enable
            if self._no_mirror:  # but we don't want one
                self.disable_mirror()

    def disable_mirror(self):
        if not self._mirror_disabled:
            _logger.debug("Temporary switching off MusicBrainz mirror")
            musicbrainzngs.set_hostname("musicbrainz.org", True)
            musicbrainzngs.set_rate_limit(True)
            self._mirror_disabled = True

    def enable_mirror(self):
        if self._mirror_disabled and not self._no_mirror:
            _logger.debug("Re-enabling MusicBrainz mirror")
            musicbrainzngs.set_hostname(self._config["api_url"], self._config["use_https"])
            musicbrainzngs.set_rate_limit(self._config["rate_limit"])
            self._mirror_disabled = False

    @contextmanager
    def without_mirror(self):
        self.disable_mirror()
        try:
            yield self
        finally:
            self.enable_mirror()

    def search_artists(self, artist_query: str, cut_off: int = 90) -> list["Artist"]:

        _logger.debug(f"Searching for artist '{artist_query}' from MusicBrainz API")

        # cache
        key = f"search_search_for_artist_for_artist_{artist_query}"
        if not self._search_cache and key in self._mb_shelf.keys():
            _logger.debug(f"Removing search for artist query '{artist_query}' from cache")
            del (self._mb_shelf[key])
        if key in self._mb_shelf.keys():
            return self._mb_shelf[key]

        search_params = {
        }

        try:
            result = []
            artist_split = split_artist(artist_query)
            _logger.debug(f"Split artist query to: {artist_split}")
            for artist_split_query in artist_split:
                with self.without_mirror():
                    response = musicbrainzngs.search_artists(artist=artist_split_query, **search_params)

                for r in response["artist-list"]:
                    score = int(r["ext:score"])
                    if score > cut_off:
                        artist_id = ArtistID(r["id"])
                        if artist_id not in result and artist_id not in [VA_ARTIST_ID, UNKNOWN_ARTIST_ID]:
                            result.append(artist_id)
            result = [self.get_artist_by_id(x) for x in result]
            result = [x for x in result if x.is_sane(artist_query, cut_off)]
            result = sorted(result, reverse=True)
            self._mb_shelf[key] = result
            _logger.debug(f"Search gave us {len(result)} results above cutoff threshold")
            return result
        except musicbrainzngs.WebServiceError as ex:
            raise MBApiError("Could not get result from musicbrainz_wrapper API") from ex

    def search_recording(
            self,
            artist_query: str,
            title_query: str,
            date: datetime.date = None,
            cut_off: int = 90) -> list["Recording"]:

        _logger.debug(f"Searching for recording '{artist_query}' - '{title_query}' from MusicBrainz API")

        # cache
        key = f"search_for_recording_{artist_query}_{title_query}"
        if not self._search_cache and key in self._mb_shelf.keys():
            _logger.debug(f"Removing search for recording query '{artist_query}' - '{title_query}' from cache")
            del (self._mb_shelf[key])
        if key in self._mb_shelf.keys():
            return self._mb_shelf[key]

        result_ids = []

        search_params = {
            "artist": artist_query,
            "alias": title_query,  # or "recording"
            "limit": 100,
            "status": str(ReleaseStatus.OFFICIAL),
            "video": False,
            "strict": True
        }

        try:

            offset = 0
            fetched = None
            with self.without_mirror():
                while fetched is None or fetched >= 100:
                    fetch_result = musicbrainzngs.search_recordings(**search_params, offset=offset)
                    fetched = len(fetch_result["recording-list"])
                    for r in fetch_result["recording-list"]:
                        score = int(r["ext:score"])
                        if score > cut_off:
                            result_ids.append((RecordingID(r["id"]), score))
                        else:
                            fetched = 0
                            break
                    offset = offset + fetched

            _logger.debug(f"Search gave us {len(result_ids)} results above cutoff threshold")

            result = []
            for rid, score in result_ids:
                try:
                    recording = self.get_recording_by_id(rid)
                    if recording.is_sane(artist_query, title_query):
                        result.append((recording, score))
                except MBApiError as ex:
                    _logger.warning(f"Could not get recording {str(rid)}")

            result = sorted(result, key=lambda x: x[1], reverse=True)
            result = [x[0] for x in result]
            self._mb_shelf[key] = result
            return result
        except musicbrainzngs.WebServiceError as ex:
            raise MBApiError("Could not get result from musicbrainz_wrapper API") from ex

    def typesense_lookup(self, artist_name, recording_name):
        hits = typesense_lookup(artist_name, recording_name)

        output = []
        for hit in hits:
            hit['artists'] = [self.get_artist_by_id(x) for x in hit['artist_ids']]
            hit['release'] = self.get_release_by_id(hit['release_id'])
            hit['recording'] = self.get_recording_by_id(hit['recording_id'])
            hit['release_group'] = hit['release'].release_group
            output.append(hit)
        return output
