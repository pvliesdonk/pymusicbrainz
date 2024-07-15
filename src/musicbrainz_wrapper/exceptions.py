import logging


class MBApiError(Exception):
    _logger = logging.getLogger(__name__)


class IncomparableError(MBApiError):
    pass


class NotFoundError(MBApiError):
    pass


class NotConfiguredError(MBApiError):
    pass
