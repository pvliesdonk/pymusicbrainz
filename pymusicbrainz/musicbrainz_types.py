from __future__ import annotations

import enum


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


PRIMARY_TYPES = {
    ReleaseType.ALBUM: 1,
    ReleaseType.SINGLE: 2,
    ReleaseType.EP: 3,
    ReleaseType.OTHER: 11,
    ReleaseType.BROADCAST: 12}


class SecondaryTypeList(list[ReleaseType]):
    def __hash__(self):
        if ReleaseType.NONE in self:
            return hash("_".join([ReleaseType.NONE]))
        if ReleaseType.ALL in self:
            return hash("_".join([ReleaseType.ALL]))
        return hash("_".join(sorted(self)))


SECONDARY_TYPES = {
    ReleaseType.COMPILATION: 1,
    ReleaseType.SOUNDTRACK: 2,
    ReleaseType.SPOKENWORD: 3,
    ReleaseType.INTERVIEW: 4,
    ReleaseType.AUDIOBOOK: 5,
    ReleaseType.LIVE: 6,
    ReleaseType.REMIX: 7,
    ReleaseType.DJ_MIX: 8,
    ReleaseType.MIXTAPE: 9,
    ReleaseType.DEMO: 10,
    ReleaseType.AUDIODRAMA: 11,
    ReleaseType.FIELDRECORDING: 12
}


class ReleaseStatus(enum.StrEnum):
    """Constants for the various Musicbrainz release statuses"""
    OFFICIAL = "official"
    PROMOTION = "promotion"
    BOOTLEG = "bootleg"
    PSEUDO = "pseudo-release"


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
