import logging

import musicbrainzngs
import urllib3

from .constants import (DEFAULT_APP, DEFAULT_VERSION, DEFAULT_CONTACT, DEFAULT_API_URL, DEFAULT_HTTPS, DEFAULT_RATE_LIMIT)
from . import db, typesense

_logger = logging.getLogger(__name__)

_musicbrainzngs_configured: bool = False


def configure_musicbrainzngs(
        app: str = DEFAULT_APP,
        version: str = DEFAULT_VERSION,
        contact: str = DEFAULT_CONTACT,
        api_url: str = DEFAULT_API_URL,
        use_https: bool = DEFAULT_HTTPS,
        rate_limit: bool = DEFAULT_RATE_LIMIT,
) -> None:
    """Configure where and how the Musicbrainz API is accessed

    :param app: Application name to be used in User Agent identifier
    :param version: Application version to be used in User Agent identifier
    :param contact: Contact name to be used in User Agent identifier
    :param api_url: API url for MusicBrainz API, e.g. when using a mirror (default musicbrainz.org:443).
    :param use_https: When True, use HTTPS instead of HTTP (default: True)
    :param rate_limit: Perform rate limiting. (default: True)
    """
    _logger.debug(
        f"Configuring MusicBrainz API access via 'http{'s' if use_https else ''}://{api_url}' with rate limiting {'enabled' if rate_limit else 'disabled'}.")
    musicbrainzngs.set_hostname(api_url, use_https=use_https)
    musicbrainzngs.set_rate_limit(rate_limit)
    musicbrainzngs.set_useragent(app=app, version=version, contact=contact)


def is_configured_musicbrainzngs() -> bool:
    """Returns True if MusicBrainz API is configured, False otherwise."""
    return _musicbrainzngs_configured


def configure_database(db_url: str = None, echo_sql: bool = False) -> None:
    """Configure the PostgreSQL database for Musicbrainz

    :param db_url: URI for PostgreSQL database
    :param echo_sql: Echo all SQL statements to stdout
    """
    db.configure_database(db_url=db_url, echo_sql=echo_sql)


def configure_typesense(url: urllib3.util.Url = None, api_key: str = None, collection: str = None,
                        search_field: str = None) -> None:
    """Configure how to access the Typesense search service

        :param url: Full URL to the Typesense server
        :param api_key: API key to connect to the Typesense server
        :param collection: Collection to query
        :param search_field: Search field to search against
        """
    return typesense.configure_typesense(url, api_key, collection, search_field)
