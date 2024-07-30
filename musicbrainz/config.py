import logging

import musicbrainzngs

from .constants import _DEFAULT_APP, _DEFAULT_VERSION, _DEFAULT_CONTACT, _DEFAULT_API_URL, \
    _DEFAULT_HTTPS, _DEFAULT_RATE_LIMIT

_logger = logging.getLogger(__name__)

_musicbrainzngs_configured: bool = False


def configure_musicbrainzngs(
        app: str = _DEFAULT_APP,
        version: str = _DEFAULT_VERSION,
        contact: str = _DEFAULT_CONTACT,
        api_url: str = _DEFAULT_API_URL,
        use_https: bool = _DEFAULT_HTTPS,
        rate_limit: bool = _DEFAULT_RATE_LIMIT,
):
    _logger.debug(
        f"Configuring MusicBrainz API access via 'http{'s' if use_https else ''}://{api_url}' with rate limiting {'enabled' if rate_limit else 'disabled'}.")
    musicbrainzngs.set_hostname(api_url, use_https=use_https)
    musicbrainzngs.set_rate_limit(rate_limit)
    musicbrainzngs.set_useragent(app=app, version=version, contact=contact)


def is_configured_musicbrainzngs() -> bool:
    return _musicbrainzngs_configured
