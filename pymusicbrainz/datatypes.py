import enum
import logging
import uuid

_logger = logging.getLogger(__name__)


class MBID(uuid.UUID):
    """Abstract representation of a Musicbrainz Identifier"""
    pass


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