import logging

import typesense
import urllib3.util
from urllib3.util import parse_url

from .datatypes import ArtistID, ReleaseID, RecordingID
from .util import flatten_title

_logger = logging.getLogger(__name__)

_url: urllib3.util.Url = urllib3.util.parse_url("http://musicbrainz.int.liesdonk.nl:8108")
_api_key: str = "xyz"

_search_field: str = "combined"
_collection: str = "musicbrainz"


_client: typesense.Client | None = None


def configure_typesense(url: urllib3.util.Url = None, api_key: str = None, collection: str = None, search_field: str = None):
    global _url, _api_key, _collection, _search_field
    if url is not None:
        _logger.info(f"Now configured to access typesense at {url}")
        _url = url

    if collection is not None:
        _logger.info(f"Now configured to read typesense collection '{collection}'")
        _collection = collection

    if search_field is not None:
        _logger.info(f"Now configured to search typesense field {search_field}")
        _search_field = search_field

    if api_key is not None:
        _api_key = api_key


def get_client():
    global _client
    if _client is None:

        _client = typesense.Client({
            'nodes': [{
                'host': _url.host,
                'port': _url.port,
                'protocol': _url.scheme,
            }],
            'api_key': _api_key,
            'connection_timeout_seconds': 1000000
        })
        _logger.debug("Connected Typesense client")
    return _client


def typesense_lookup(artist_name, recording_name):
    """ Perform a lookup on the typsense index """

    client = get_client()
    query = flatten_title(artist_name, recording_name)
    search_parameters = {'q': query, 'query_by': _search_field, 'prefix': 'no', 'num_typos': 5}

    _logger.debug(f"Search typesense collection {_collection} for '{_search_field}'~='{query}'.")
    hits = client.collections[_collection].documents.search(search_parameters)

    output = []
    for hit in hits['hits']:
        doc = hit['document']
        acn = doc['artist_credit_name']
        artist_ids = doc['artist_mbids'].split(',')
        release_id = doc['release_mbid']

        recording_id = doc['recording_mbid']
        output.append(
            {
                'artist_credit_name': acn,
                'artist_ids': [ArtistID(x) for x in artist_ids],
                'release_id': ReleaseID(release_id),
                'recording_id': RecordingID(recording_id)
            }
        )

    return output

