import logging
import logging
import pathlib
from typing import Sequence, Optional

import acoustid
import musicbrainzngs

from .constants import VA_ARTIST_ID, UNKNOWN_ARTIST_ID, ACOUSTID_APIKEY, ACOUSTID_META
from .dataclasses import Recording, Artist, MusicbrainzSearchResult, \
    MusicbrainzSingleResult, MusicbrainzListResult
from .datatypes import ReleaseStatus, RecordingID, ArtistID, SearchType
from .exceptions import MBApiError
from .object_cache import get_recording, get_artist, get_release
from .typesense import do_typesense_lookup
from .util import split_artist, recording_redirect

_logger = logging.getLogger(__name__)


def search_song_musicbrainz(
        artist_query: str | Artist,
        title_query: str,
        strict: bool = True,
        cut_off: int = 90) -> list["Recording"]:
    """Search for a recording in the Musicbrainz API

    :param artist_query: Artist name
    :param title_query: Recording name / Title
    :param strict: Do a strict AND search
    :param cut_off: Tweak on when to cut of results from search (optional)
    :return:
    """
    _logger.debug(f"Searching for recording '{artist_query}' - '{title_query}' from MusicBrainz API")

    result_ids = []

    search_params = {
        "recording": title_query,  # "alias" or "recording"
        "limit": 100,
        "status": str(ReleaseStatus.OFFICIAL),
        "video": False,
        "strict": strict
    }

    if isinstance(artist_query, Artist):
        search_params["arid"] = str(artist_query.id)
    if isinstance(artist_query, str):
        search_params["artist"] = artist_query

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

        normalized_result = [x for x in result if x.is_normal_performance]
        if len(normalized_result) > 0:
            _logger.debug(f"Shortened to {len(normalized_result)} results that are normal performances")
            result = normalized_result
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
        _logger.debug(f"Search gave us {len(result)} results above cutoff threshold")
        return result
    except musicbrainzngs.WebServiceError as ex:
        raise MBApiError("Could not get result from musicbrainz_wrapper API") from ex


def search_song_canonical(
        artist_query: str,
        title_query: str,
) -> Optional[MusicbrainzListResult]:
    """Search for a recording in the list of canonical releases

    :param artist_query: Artist name
    :param title_query: Recording name / Title
    :return:
    """
    _logger.debug("Doing a lookup for canonical release")
    canonical_hits = _search_typesense(artist_query, title_query)
    if len(canonical_hits) > 0:
        _logger.info(f"Found canonical release for '{artist_query}' - '{title_query}'")
        result: MusicbrainzListResult = MusicbrainzListResult()
        for hit in canonical_hits:
            r = MusicbrainzSingleResult(release_group=hit['release_group'], recording=hit['recording'])
            result.append(r)
        result.sort()
        return result
    else:
        _logger.info(f"No canonical release found for '{artist_query}' - '{title_query}' ")
        return None


def _search_release_group_by_recording_ids(
        recording_ids: RecordingID | Sequence[RecordingID],
        search_type: SearchType,
        use_siblings: bool = True,
        cut_off: int = None
) -> Optional[MusicbrainzListResult]:
    if cut_off is None:
        cut_off = 97

    # get actual MB objects
    if isinstance(recording_ids, RecordingID):
        recordings = [get_recording(recording_ids)]
    else:
        recordings = [get_recording(x) for x in recording_ids]

    # check whether there are normal performances on board. Kill the others
    if any([r.is_normal_performance for r in recordings]):
        recordings = [r for r in recordings if r.is_normal_performance]

    # also search for recording siblings
    if use_siblings:
        new_recordings = recordings.copy()
        for recording in recordings:
            for sibling in recording.siblings:
                if sibling not in new_recordings:
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

    found_rgs: MusicbrainzListResult = MusicbrainzListResult()
    for artist in artists:
        for rg in getattr(artist, search_field):
            for recording in recordings:
                if recording in rg:
                    single_result = MusicbrainzSingleResult(release_group=rg, recording=recording)
                    if single_result not in found_rgs:
                        #_logger.debug(f"Found track {track.position}. {recording.artist_credit_phrase} - {recording.title}")
                        found_rgs.append(single_result)

    found_rgs.sort()
    if len(found_rgs) > 0:
        _logger.info(f"Found {found_rgs[0].track} for searchtype {search_type}")
        return found_rgs
    else:
        _logger.debug(f"No release groups found for search type {search_type}")
        return None


def search_studio_albums_by_recording_ids(
        recording_ids: RecordingID | Sequence[RecordingID],
        use_siblings: bool = True,
        cut_off: int = None
) -> Optional[MusicbrainzListResult]:
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
) -> Optional[MusicbrainzListResult]:
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
) -> Optional[MusicbrainzListResult]:
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
) -> Optional[MusicbrainzListResult]:
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
) -> Optional[MusicbrainzListResult]:
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

) -> MusicbrainzSearchResult:
    results: MusicbrainzSearchResult = MusicbrainzSearchResult()
    for search_type in SearchType:
        if search_type in [SearchType.CANONICAL, SearchType.ALL]:
            continue
        res = _search_release_group_by_recording_ids(
            recording_ids=recording_ids,
            search_type=SearchType(search_type),
            use_siblings=use_siblings,
            cut_off=cut_off)
        if res is not None:
            results.add_result(search_type, res)

    return results


def _recording_id_from_fingerprint(file: pathlib.Path, cut_off: int = None) -> list[RecordingID]:
    if cut_off is None:
        cut_off = 97

    try:
        duration, fp = acoustid.fingerprint_file(path=str(file))
    except acoustid.FingerprintGenerationError as ex:
        _logger.error(f"Could not compute fingerprint for file '{file}'")
        raise MBApiError(f"Could not compute fingerprint for file '{file}'") from ex

    try:
        response = acoustid.lookup(apikey=ACOUSTID_APIKEY, fingerprint=fp, duration=duration, meta=ACOUSTID_META)
    except acoustid.WebServiceError as ex:
        _logger.error("Could not obtain Acoustid fingerprint from webservice")
        raise MBApiError("Could not obtain Acoustid fingerprint from webservice") from ex

    if response['status'] != 'ok':
        raise MBApiError("Could not obtain Acoustid fingerprint from webservice")

    recording_ids = []
    for result in response['results']:
        if result['score'] < cut_off / 100:
            continue
        _logger.debug(f"Processing acoustid https://acoustid.org/track/{result['id']}")
        recordings = sorted(result["recordings"], key=lambda x: x['sources'], reverse=True)
        previous_score = None
        for rec in recordings:

            redirected_id = recording_redirect(rec['id'])
            recording = get_recording(redirected_id)

            if previous_score is not None and (previous_score - rec['sources']) > 10:
                # print(f"Acoustid: dropping {recording} ({rec['sources']} sources)")
                continue
            if redirected_id not in recording_ids:
                print(f"Acoustid: adding {recording} ({rec['sources']} sources)")
                recording_ids.append(redirected_id)
            previous_score = rec['sources']
    return recording_ids





def search_fingerprint(file: pathlib.Path, cut_off: int = None) \
        -> MusicbrainzSearchResult:
    recording_ids = _recording_id_from_fingerprint(file=file, cut_off=cut_off)

    recordings = [get_recording(x) for x in recording_ids]

    result =  search_by_recording_id(recording_ids)

    _logger.info(f"Also trying canonical search using fingerprint search result")
    k: SearchType
    v: MusicbrainzSingleResult
    for k, v in result.iterate_results():
        canonical = search_song_canonical(artist_query=v.recording.artist_credit_phrase, title_query=v.recording.title)
        if canonical is not None:
            _logger.debug(f"Found canonical result via search for {k.name}")
            result.add_result(SearchType.CANONICAL, canonical)
            break
    return result


def search_fingerprint_by_type(
        file: pathlib.Path,
        search_type: SearchType,
        use_siblings: bool = True,
        cut_off: int = None) -> MusicbrainzListResult:
    recording_ids = _recording_id_from_fingerprint(file=file, cut_off=cut_off)

    return _search_release_group_by_recording_ids(
        recording_ids=recording_ids,
        search_type=search_type,
        use_siblings=use_siblings,
        cut_off=cut_off
    )


def search_song(artist_query: str, title_query: str, cut_off: int = None) \
        -> Optional[MusicbrainzSearchResult]:
    """Main search function

    :param artist_query: Artist name
    :param title_query: Recording name / Title
    :param cut_off:
    :return:
    """
    if cut_off is None:
        cut_off = 90

    canonical: MusicbrainzListResult = search_song_canonical(artist_query=artist_query, title_query=title_query)

    songs_found: list[Recording] = search_song_musicbrainz(artist_query=artist_query, title_query=title_query,
                                                           cut_off=cut_off, strict=True)

    if len(songs_found) == 0:
        _logger.info(f"Trying less restrictive search")
        songs_found = search_song_musicbrainz(artist_query=artist_query, title_query=title_query, cut_off=cut_off,
                                              strict=False)
    recording_ids = [recording.id for recording in songs_found]  # if recording.is_sane(artist_query, title_query)

    if len(recording_ids) == 0:
        if canonical is not None:
            _logger.info(
                f"Searching for '{artist_query}' - '{title_query}' gave no results. Triggering search from canonical result {canonical[0].recording}.")
            recording_ids = [canonical[0].recording.id]
        else:
            _logger.info(
                f"No recordings found for '{artist_query}' - '{title_query}'. Trying artist search to determine a different artist")
            artists = search_artist_musicbrainz(artist_query=artist_query, cut_off=80)
            for artist in artists:
                songs_found = search_song_musicbrainz(artist_query=artist, title_query=title_query, cut_off=cut_off)
                recording_ids.extend(
                    [recording.id for recording in songs_found])  # if recording.is_sane(artist_query, title_query)

        if len(recording_ids) == 0:
            _logger.error(f"No  recordings found for '{artist_query}' - '{title_query}'")
            return None

    result: MusicbrainzSearchResult = search_by_recording_id(recording_ids)

    if canonical is not None:
        result.add_result(SearchType.CANONICAL, canonical)
    elif not result.is_empty():
        _logger.info(f"Retrying failed canonical search using search result")
        k: SearchType
        v: MusicbrainzSingleResult
        for k, v in result.iterate_results():
            canonical = search_song_canonical(artist_query=v.recording.artist_credit_phrase, title_query=v.recording.title)
            if canonical is not None:
                _logger.debug(f"Found canonical result via search for {k.name}")
                result.add_result(SearchType.CANONICAL, canonical)
                break
    return result


def search_name_by_type(
        artist_query: str,
        title_query: str,
        search_type: SearchType,
        use_siblings: bool = True,
        cut_off: int = None) -> MusicbrainzListResult:
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


