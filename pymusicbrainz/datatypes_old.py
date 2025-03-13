import enum
import logging

_logger = logging.getLogger(__name__)


class SearchType(enum.StrEnum):
    """Constant to define what sort of search to perform"""
    CANONICAL = "canonical"
    STUDIO_ALBUM = "studio_album"
    SINGLE = "single"
    SOUNDTRACK = "soundtrack"
    EP = "ep"
    ALL = "all"


