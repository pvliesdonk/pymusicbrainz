import enum
import logging
import uuid

import sqlalchemy

_logger = logging.getLogger(__name__)


class ReleaseType(enum.StrEnum):
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


class ReleaseStatus(enum.StrEnum):
    OFFICIAL = "Official"
    PROMOTION = "Promotion"
    BOOTLEG = "Bootleg"
    PSEUDO = "Pseudo-Release"


class MBID(uuid.UUID):
    pass


class ArtistID(MBID):
    pass


class ReleaseID(MBID):
    pass


class ReleaseGroupID(MBID):
    pass


class RecordingID(MBID):
    pass


class WorkID(MBID):
    pass


class MediumID(MBID):
    pass


class TrackID(MBID):
    pass


UNKNOWN_ARTIST_ID = ArtistID("125ec42a-7229-4250-afc5-e057484327fe")
VA_ARTIST_ID = ArtistID("89ad4ac3-39f7-470e-963a-56509c546377")
