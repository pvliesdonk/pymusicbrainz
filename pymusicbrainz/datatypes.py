import enum
import logging

_logger = logging.getLogger(__name__)


class ReleaseType(enum.StrEnum):
    """Constants for the different Musicbrainz release types"""
    NAT = "Nat"
    ALBUM = "Album"
    SINGLE = "Single"
    EP = "EP"
    BROADCAST = "Broadcast"
    OTHER = "Other"
    COMPILATION = "Compilation"
    SOUNDTRACK = "Soundtrack"
    SPOKENWORD = "Spokenword"
    INTERVIEW = "Interview"
    AUDIOBOOK = "Audiobook"
    LIVE = "Live"
    REMIX = "Remix"
    DJ_MIX = "DJ-mix"
    MIXTAPE = "Mixtape/Street"
    DEMO = "Demo"
    AUDIODRAMA = "Audio drama"
    FIELDRECORDING = "Field recording"

    ALL = "All"
    NONE = "None"


class SecondaryTypeList(list[ReleaseType]):
    def __hash__(self):
        if ReleaseType.NONE in self:
            return hash("_".join([ReleaseType.NONE]))
        if ReleaseType.ALL in self:
            return hash("_".join([ReleaseType.ALL]))
        return hash("_".join(sorted(self)))


class ReleaseStatus(enum.StrEnum):
    """Constants for the various Musicbrainz release statuses"""
    OFFICIAL = "Official"
    PROMOTION = "Promotion"
    BOOTLEG = "Bootleg"
    PSEUDO = "Pseudo-Release"


class SearchType(enum.StrEnum):
    """Constant to define what sort of search to perform"""
    CANONICAL = "canonical"
    STUDIO_ALBUM = "studio_album"
    SINGLE = "single"
    SOUNDTRACK = "soundtrack"
    EP = "ep"
    ALL = "all"


class PerformanceWorkAttributes(enum.StrEnum):
    ACAPPELLA = "acappella"
    COVER = "cover"
    INSTRUMENTAL = "instrumental"
    KARAOKE = "karaoke"
    LIVE = "live"
    MEDLEY = "medley"
    PARTIAL = "partial"
    NONE = "no-attr"
    ALL = "all"
