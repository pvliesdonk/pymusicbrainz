from . import ArtistID, ReleaseType

PRIMARY_TYPES = {
    ReleaseType.ALBUM: 1,
    ReleaseType.SINGLE: 2,
    ReleaseType.EP: 3,
    ReleaseType.OTHER: 11,
    ReleaseType.BROADCAST: 12}
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
UNKNOWN_ARTIST_ID = ArtistID("125ec42a-7229-4250-afc5-e057484327fe")
VA_ARTIST_ID = ArtistID("89ad4ac3-39f7-470e-963a-56509c546377")
