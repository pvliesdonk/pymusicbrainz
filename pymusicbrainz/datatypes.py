import enum
import logging
import uuid

_logger = logging.getLogger(__name__)


class MBID:
    """Abstract representation of a Musicbrainz Identifier"""

    def __init__(self, mbid: str | uuid.UUID):

        if isinstance(mbid, str):
            self.mbid: uuid.UUID = uuid.UUID(mbid)

        elif isinstance(mbid, uuid.UUID):
            self.mbid = mbid

        else:
            _logger.error("SOMETHING IS WRONG!!!")

    def __repr__(self):
        return f"{type(self).__name__}({str(self.mbid)})"

    def __str__(self):
        return str(self.mbid)

    def __hash__(self):
        return hash(self.mbid.hex)

    def __eq__(self, other):
        return self.mbid == other.mbid

    @property
    def hex(self) -> str:
        return self.mbid.hex

    def encode(self, encoding:str = "utf-8", errors: str = "strict") -> bytes:
        return str(self.mbid).encode(encoding, errors)


class ArtistID(MBID):
    """Musicbrainz Artist ID"""
    pass


class ReleaseGroupID(MBID):
    """Musicbrainz Release Group ID"""
    pass


class ReleaseID(MBID):
    """Musicbrainz Release ID"""
    pass


class RecordingID(MBID):
    """Musicbrainz Recording ID"""
    pass


class MediumID(MBID):
    """Musicbrainz Medium ID"""
    pass


class TrackID(MBID):
    """Musicbrainz Track ID"""
    pass


class WorkID(MBID):
    """Musicbrainz Work ID"""
    pass


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
    EXTENDED_ALBUM = "extended_album"
    COMPILATION = "compilation"
    IMPORT = "import"  # not used in this search
    MANUAL = "manual" # not used in this search


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
    DEMO = "demo"
