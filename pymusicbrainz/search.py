import datetime
import logging
import pathlib
from typing import Sequence

import acoustid
import musicbrainzngs

from .constants import VA_ARTIST_ID, UNKNOWN_ARTIST_ID, ACOUSTID_APIKEY
from .dataclasses import Recording, Artist, ReleaseGroup, MusicBrainzObject, Release, Track
from .datatypes import ReleaseStatus, RecordingID, ArtistID, SearchType
from .exceptions import MBApiError
from .typesense import do_typesense_lookup
from .object_cache import get_recording, get_artist, get_release
from .util import split_artist

_logger = logging.getLogger(__name__)


def search_song_musicbrainz(
        artist_query: str,
        title_query: str,
        date: datetime.date = None,
        cut_off: int = 90) -> list["Recording"]:
    """Search for a recording in the Musicbrainz API

    :param artist_query: Artist name
    :param title_query: Recording name / Title
    :param date: Date of release (optional)
    :param cut_off: Tweak on when to cut of results from search (optional
    :return:
    """
    _logger.debug(f"Searching for recording '{artist_query}' - '{title_query}' from MusicBrainz API")

    result_ids = []

    search_params = {
        "artist": artist_query,
        "alias": title_query,  # or "recording"
        "limit": 100,
        "status": str(ReleaseStatus.OFFICIAL),
        "video": False,
        "strict": True
    }

    try:

        offset = 0
        fetched = None
        while fetched is None or fetched >= 100:
            fetch_result = musicbrainzngs.search_recordings(**search_params, offset=offset)
            fetched = len(fetch_result["recording-list"])
            for r in fetch_result["recording-list"]:
                score = int(r["ext:score"])
                if score > cut_off:
                    result_ids.append((RecordingID(r["id"]), score))
                else:
                    fetched = 0
                    break
            offset = offset + fetched

        _logger.debug(f"Search gave us {len(result_ids)} results above cutoff threshold")

        result = []
        for rid, score in result_ids:
            try:
                recording = get_recording(rid)
                if recording.is_sane(artist_query, title_query):
                    result.append((recording, score))
            except MBApiError as ex:
                _logger.warning(f"Could not get recording {str(rid)}")

        result = sorted(result, key=lambda x: x[1], reverse=True)
        result = [x[0] for x in result]
        return result
    except musicbrainzngs.WebServiceError as ex:
        raise MBApiError("Could not get result from musicbrainz_wrapper API") from ex


def _search_typesense(artist_name, recording_name):
    hits = do_typesense_lookup(artist_name, recording_name)

    output = []
    for hit in hits:
        hit['artists'] = [get_artist(x) for x in hit['artist_ids']]
        hit['release'] = get_release(hit['release_id'])
        hit['recording'] = get_recording(hit['recording_id'])
        hit['release_group'] = hit['release'].release_group
        output.append(hit)
    return output


def search_artist_musicbrainz(artist_query: str, cut_off: int = 90) -> list["Artist"]:
    """Search for a recording in the Musicbrainz API

    :param artist_query: Artist name
    :param cut_off: Tweak on when to cut of results from search (optional
    :return:
    """
    _logger.debug(f"Searching for artist '{artist_query}' from MusicBrainz API")

    search_params = {}

    try:
        result = []
        artist_split = split_artist(artist_query)
        _logger.debug(f"Split artist query to: {artist_split}")
        for artist_split_query in artist_split:
            response = musicbrainzngs.search_artists(artist=artist_split_query, **search_params)

            for r in response["artist-list"]:
                score = int(r["ext:score"])
                if score > cut_off:
                    artist_id = ArtistID(r["id"])
                    if artist_id not in result and artist_id not in [VA_ARTIST_ID, UNKNOWN_ARTIST_ID]:
                        result.append(artist_id)
        result = [get_artist(x) for x in result]
        result = [x for x in result if x.is_sane(artist_query, cut_off)]
        result = sorted(result, reverse=True)
        _logger.debug(f"Search gave us {len(result)} results above cutoff threshold")
        return result
    except musicbrainzngs.WebServiceError as ex:
        raise MBApiError("Could not get result from musicbrainz_wrapper API") from ex


def search_song_canonical(
        artist_query: str,
        title_query: str,
) -> dict[str, MusicBrainzObject] | None:
    """Search for a recording in the list of canonical releases

    :param artist_query: Artist name
    :param title_query: Recording name / Title
    :return:
    """
    _logger.debug("Doing a lookup for canonical release")
    canonical_hits = _search_typesense(artist_query, title_query)
    if len(canonical_hits) > 0:
        _logger.info("Found canonical release according to MusicBrainz Canonical dataset")
        rg: ReleaseGroup = canonical_hits[0]['release_group']
        recording: Recording = canonical_hits[0]['recording']
        release: Release = canonical_hits[0]['release']
        track: Track = find_track_for_release_recording(release, recording)
        return {"release_group": rg,
                "recording": recording,
                "release": release,
                "track": track
                }
    else:
        return None


def _search_release_group_by_recording_ids(
        recording_ids: RecordingID | Sequence[RecordingID],
        search_type: SearchType,
        use_siblings: bool = True,
        cut_off: int = None
) -> dict[str, MusicBrainzObject] | None:
    if cut_off is None:
        cut_off = 97

    # get actual MB objects
    if isinstance(recording_ids, RecordingID):
        recordings = [get_recording(recording_ids)]
    else:
        recordings = [get_recording(x) for x in recording_ids]

    # also search for recording siblings
    if use_siblings:
        new_recordings = []
        for recording in recordings:
            for sibling in recording.siblings:
                if sibling not in recordings:
                    new_recordings.append(sibling)
        recordings = new_recordings

    match search_type:
        case SearchType.CANONICAL:
            return None
        case SearchType.STUDIO_ALBUM:
            search_field = "studio_albums"
        case SearchType.SINGLE:
            search_field = "singles"
        case SearchType.SOUNDTRACK:
            search_field = "soundtracks"
        case SearchType.EP:
            search_field = "eps"
        case SearchType.ALL | _:
            search_field = "release_groups"

    # find the actual release groups
    artists = []
    for recording in recordings:
        for artist in recording.artists:
            if artist not in artists:
                artists.append(artist)

    found_rgs = []
    for artist in artists:
        for rg in getattr(artist, search_field):
            _logger.debug(f"Searching in '{rg.artist_credit_phrase}' - '{rg.title}' ")
            for recording in recordings:
                if recording in rg:
                    track, release = find_track_release_for_release_group_recording(rg, recording)
                    if (rg, recording, release, track) not in found_rgs:
                        _logger.debug(f"Found track {track.position}. {recording.artist_credit_phrase} - {recording.title}")
                        found_rgs.append((rg, recording, release, track))

    found_rgs = sorted(found_rgs, key=lambda x: (x[0], x[2], x[3].position))
    if len(found_rgs) > 0:
        _logger.debug(f"Found {found_rgs[0][3]}")
        return {
            "release_group": found_rgs[0][0],
            "recording": found_rgs[0][1],
            "release": found_rgs[0][2],
            "track": found_rgs[0][3]
        }
    else:
        return None


def search_studio_albums_by_recording_ids(
        recording_ids: RecordingID | Sequence[RecordingID],
        use_siblings: bool = True,
        cut_off: int = None
) -> dict[str, MusicBrainzObject] | None:
    return _search_release_group_by_recording_ids(
        recording_ids=recording_ids,
        search_type=SearchType.STUDIO_ALBUM,
        use_siblings=use_siblings,
        cut_off=cut_off
    )


def search_soundtracks_by_recording_ids(
        recording_ids: RecordingID | Sequence[RecordingID],
        use_siblings: bool = True,
        cut_off: int = None
) -> dict[str, MusicBrainzObject] | None:
    return _search_release_group_by_recording_ids(
        recording_ids=recording_ids,
        search_type=SearchType.SOUNDTRACK,
        use_siblings=use_siblings,
        cut_off=cut_off
    )


def search_eps_by_recording_ids(
        recording_ids: RecordingID | Sequence[RecordingID],
        use_siblings: bool = True,
        cut_off: int = None
) -> dict[str, MusicBrainzObject] | None:
    return _search_release_group_by_recording_ids(
        recording_ids=recording_ids,
        search_type=SearchType.EP,
        use_siblings=use_siblings,
        cut_off=cut_off
    )


def search_singles_by_recording_ids(
        recording_ids: RecordingID | Sequence[RecordingID],
        use_siblings: bool = True,
        cut_off: int = None
) -> dict[str, MusicBrainzObject] | None:
    return _search_release_group_by_recording_ids(
        recording_ids=recording_ids,
        search_type=SearchType.SINGLE,
        use_siblings=use_siblings,
        cut_off=cut_off
    )


def search_release_groups_by_recording_ids(
        recording_ids: RecordingID | Sequence[RecordingID],
        use_siblings: bool = True,
        cut_off: int = None
) -> dict[str, MusicBrainzObject] | None:
    return _search_release_group_by_recording_ids(
        recording_ids=recording_ids,
        search_type=SearchType.ALL,
        use_siblings=use_siblings,
        cut_off=cut_off
    )


def search_by_recording_id(
        recording_ids: RecordingID | Sequence[RecordingID],
        use_siblings: bool = True,
        cut_off: int = None

) -> dict[SearchType, dict[str, MusicBrainzObject]]:
    results = {
        search_type: _search_release_group_by_recording_ids(
            recording_ids=recording_ids,
            search_type=SearchType(search_type),
            use_siblings=use_siblings,
            cut_off=cut_off) for search_type in SearchType if search_type not in [SearchType.CANONICAL, SearchType.ALL]}

    return {k: v for k, v in results.items() if v is not None}


def _recording_id_from_fingerprint(file: pathlib.Path, cut_off: int = None) -> list[RecordingID]:
    if cut_off is None:
        cut_off = 97

    recording_ids = []
    try:
        for score, recording_id, title, artist in acoustid.match(ACOUSTID_APIKEY, str(file)):
            if score > cut_off / 100:
                recording_ids.append(RecordingID(recording_id))
        return recording_ids
    except acoustid.FingerprintGenerationError as ex:
        _logger.error(f"Could not compute fingerprint for file '{file}'")
        raise MBApiError(f"Could not compute fingerprint for file '{file}'") from ex
    except acoustid.WebServiceError as ex:
        _logger.error("Could not obtain Acoustid fingerprint from webservice")
        raise MBApiError("Could not obtain Acoustid fingerprint from webservice") from ex


def search_fingerprint(file: pathlib.Path, cut_off: int = None) \
        -> dict[SearchType, dict[str, MusicBrainzObject]]:
    recording_ids = _recording_id_from_fingerprint(file=file, cut_off=cut_off)
    return search_by_recording_id(recording_ids)


def search_fingerprint_by_type(
        file: pathlib.Path,
        search_type: SearchType,
        use_siblings: bool = True,
        cut_off: int = None) -> dict[str, MusicBrainzObject]:
    recording_ids = _recording_id_from_fingerprint(file=file, cut_off=cut_off)

    return _search_release_group_by_recording_ids(
        recording_ids=recording_ids,
        search_type=search_type,
        use_siblings=use_siblings,
        cut_off=cut_off
    )


def search_song(artist_query: str, title_query: str, cut_off: int = None) \
        -> dict[SearchType, dict[str, MusicBrainzObject]]:
    """Main search function

    :param artist_query: Artist name
    :param title_query: Recording name / Title
    :param cut_off:
    :return:
    """
    if cut_off is None:
        cut_off = 90

    canonical = search_song_canonical(artist_query=artist_query, title_query=title_query)

    songs_found = search_song_musicbrainz(artist_query=artist_query, title_query=title_query, cut_off=cut_off)
    recording_ids = [recording.id for recording in songs_found if recording.is_sane(artist_query, title_query)]
    result = search_by_recording_id(recording_ids)
    result[SearchType.CANONICAL] = canonical

    return result


def search_name_by_type(
        artist_query: str,
        title_query: str,
        search_type: SearchType,
        use_siblings: bool = True,
        cut_off: int = None) -> dict[str, MusicBrainzObject]:
    songs_found = search_song_musicbrainz(
        artist_query=artist_query,
        title_query=title_query,
        cut_off=cut_off)

    recording_ids = [recording.id for recording in songs_found if recording.is_sane(artist_query, title_query)]

    return _search_release_group_by_recording_ids(
        recording_ids=recording_ids,
        search_type=search_type,
        use_siblings=use_siblings,
        cut_off=cut_off
    )


def find_track_for_release_recording(release: Release, recording: Recording) -> Track:
    for track in release.tracks:
        if track.recording == recording:
            return track


def find_track_release_for_release_group_recording(rg: ReleaseGroup, recording: Recording) -> tuple[Track, Release]:
    for r in rg.releases:
        for track in r.tracks:
            if track.recording == recording:
                return track, r
