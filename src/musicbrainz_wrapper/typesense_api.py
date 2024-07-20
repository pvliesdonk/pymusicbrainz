import logging

import typesense
import urllib3.util
from urllib3.util import parse_url

from .datatypes import ArtistID, ReleaseID, RecordingID
from .util import flatten_title

TYPESENSE_COLLECTION = "musicbrainz"

_logger = logging.getLogger(__name__)

_url: urllib3.util.Url = urllib3.util.parse_url("http://musicbrainz.int.liesdonk.nl:8108")
_api_key: str = "xyz"
_client: typesense.Client | None = None


def configure_typesense(url: urllib3.util.Url = None, api_key: str = None):
    global _url, _api_key
    if url is not None:
        _url = url

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
    return _client


def typesense_lookup(artist_name, recording_name):
    """ Perform a lookup on the typsense index """

    client = get_client()
    query = flatten_title(artist_name, recording_name)
    search_parameters = {'q': query, 'query_by': "combined", 'prefix': 'no', 'num_typos': 5}

    hits = client.collections[TYPESENSE_COLLECTION].documents.search(search_parameters)

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

