import logging
from typing import Any

import typesense
import urllib3.util
from typesense.exceptions import TypesenseClientError

from . import constants
from .datatypes import ArtistID, ReleaseID, RecordingID
from .util import flatten_title


_typesense_url: urllib3.util.Url = constants.DEFAULT_TYPESENSE_URL
_typesense_api_key: str = constants.DEFAULT_TYPESENSE_API_KEY
_typesense_search_field: str = constants.DEFAULT_TYPESENSE_SEARCH_FIELD
_typesense_collection: str = constants.DEFAULT_TYPESENSE_COLLECTION

_typesense_client: typesense.Client = None

_logger = logging.getLogger(__name__)


def configure_typesense(
        url: urllib3.util.Url = None,
        api_key: str = None,
        collection: str = None,
        search_field: str = None) -> None:
    """Configure how to access the Typesense search service

    :param url: Full URL to the Typesense server
    :param api_key: API key to connect to the Typesense server
    :param collection: Collection to query
    :param search_field: Search field to search against
    """
    global _typesense_url, _typesense_api_key, _typesense_collection, _typesense_search_field
    if url is not None:
        _logger.info(f"Now configured to access typesense at {url}")
        _typesense_url = url

    if collection is not None:
        _logger.info(f"Now configured to read typesense collection '{collection}'")
        _typesense_collection = collection

    if search_field is not None:
        _logger.info(f"Now configured to search typesense field {search_field}")
        _typesense_search_field = search_field

    if api_key is not None:
        _typesense_api_key = api_key


def _get_typesense_client() -> typesense.Client:
    """Returns a client to interact with the Typesense service

    :return: typesense.Client instance
    """
    global _typesense_client
    if _typesense_client is None:
        _typesense_client = typesense.Client({
            'nodes': [{
                'host': _typesense_url.host,
                'port': _typesense_url.port,
                'protocol': _typesense_url.scheme,
            }],
            'api_key': _typesense_api_key,
            'connection_timeout_seconds': 300
        })
        _logger.debug("Connected Typesense client")
    return _typesense_client


def do_typesense_lookup(artist_name, recording_name) -> list[dict[str, Any]]:
    """ Perform a lookup on the typesense index
    :param artist_name: Artist Name
    :param recording_name: Recording Name / Title
    :return: List of search results as Dicts with keys 'artist_credit_name', 'artist_ids', 'release_id' and 'recording_id'
    """
    try:
        client = _get_typesense_client()
        query = flatten_title(artist_name, recording_name)
        search_parameters = {'q': query, 'query_by': _typesense_search_field, 'prefix': 'no', 'num_typos': 5}

        _logger.debug(f"Search typesense collection {_typesense_collection} for '{_typesense_search_field}'~='{query}'.")
        hits = client.collections[_typesense_collection].documents.search(search_parameters)

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
    except TypesenseClientError as ex:
        _logger.exception("Could not get a response from Typesense server")
        return []