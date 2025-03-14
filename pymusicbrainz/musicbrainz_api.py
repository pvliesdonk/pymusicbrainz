from __future__ import annotations

import logging

import musicbrainzngs

from pymusicbrainz import config

_logger = logging.getLogger(__name__)

_musicbrainzngs_configured: bool = False


def configure_musicbrainzngs(
        app: str = config.DEFAULT_APP,
        version: str = config.DEFAULT_VERSION,
        contact: str = config.DEFAULT_CONTACT,
        api_url: str = config.DEFAULT_API_URL,
        use_https: bool = config.DEFAULT_HTTPS,
        rate_limit: bool = config.DEFAULT_RATE_LIMIT,
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
    musicbrainzngs.set_format(fmt='xml')

    global _musicbrainzngs_configured
    _musicbrainzngs_configured = True


def is_configured_musicbrainzngs() -> bool:
    """Returns True if MusicBrainz API is configured, False otherwise."""
    return _musicbrainzngs_configured

