import logging
import logging
import pathlib
from typing import Sequence, Optional

import acoustid
import musicbrainzngs

from . import find_hint_recording
from .constants import VA_ARTIST_ID, UNKNOWN_ARTIST_ID, ACOUSTID_APIKEY, ACOUSTID_META
from .dataclasses import Recording, Artist, MusicbrainzSearchResult, \
    MusicbrainzSingleResult, MusicbrainzListResult, Release, ReleaseGroup
from .datatypes import ReleaseStatus, RecordingID, ArtistID, SearchType, ReleaseType, PerformanceWorkAttributes
from .exceptions import MBApiError, IllegalArgumentError, IllegaleRecordingReleaseGroupCombination
from .object_cache import get_recording, get_artist, get_release
from .typesense import do_typesense_lookup
from .util import split_artist, recording_redirect, artist_redirect, release_redirect, title_is_live

_logger = logging.getLogger(__name__)


def search_song_musicbrainz(
        artist_query: str | Artist,
        title_query: str,
        strict: bool = True,
        limit: int = 20,
        secondary_type: ReleaseType = None,
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
        search_params["query"] = artist_query

    if secondary_type is not None:
        search_params["secondarytype"] = str(secondary_type).lower()

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
                recording = get_recording(recording_redirect(rid))
                if recording.is_sane(artist_query, title_query):
                    result.append((recording, score))
                #if len(result) > limit:
                #    _logger.warning(f"Reach maximum of {limit} result")
                #    break
            except MBApiError as ex:
                _logger.debug(f"Could not get recording {str(rid)}")

        result = sorted(result, key=lambda x: x[1], reverse=True)
        result = [x[0] for x in result]
        return result

    except musicbrainzngs.WebServiceError as ex:
        raise MBApiError("Could not get result from musicbrainz_wrapper API") from ex


def _search_typesense(artist_name, recording_name):
    hits = do_typesense_lookup(artist_name, recording_name)

    output = []
    for hit in hits:
        try:
            hit['artists'] = [get_artist(artist_redirect(x)) for x in hit['artist_ids']]
            hit['release'] = get_release(release_redirect(hit['release_id']))
            hit['recording'] = get_recording(recording_redirect(hit['recording_id']))
            hit['release_group'] = hit['release'].release_group
            output.append(hit)
        except MBApiError as ex:
            _logger.error(f"Could not process hit from typesense response")
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
                    artist_id: ArtistID = ArtistID(r["id"])
                    if artist_id not in result and artist_id not in [VA_ARTIST_ID, UNKNOWN_ARTIST_ID]:
                        result.append(artist_id)
        result = [get_artist(artist_redirect(x)) for x in result]
        result = [x for x in result if x.is_sane(artist_query, cut_off)]
        _logger.debug(f"Search gave us {len(result)} results above cutoff threshold")
        return result
    except musicbrainzngs.WebServiceError as ex:
        raise MBApiError("Could not get result from musicbrainz_wrapper API") from ex


def search_song_canonical(
        artist_query: str,
        title_query: str,
        live: bool = False,
        year: int = None
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
            try:
                release: Release = hit['release']
                release_group: ReleaseGroup = hit['release_group']
                recording: Recording = hit['recording']

                r = MusicbrainzSingleResult(release=release, release_group=release_group,
                                            recording=recording)
                result.append(r)
            except IllegaleRecordingReleaseGroupCombination as ex:
                _logger.error(ex)
                _logger.error("Could not match canonical result to database. Likely due to updated references.")
        result.sort(live=live, year=year)
        return result
    else:
        _logger.info(f"No canonical release found for '{artist_query}' - '{title_query}' ")
        return None


def _search_release_group_by_recording_ids(
        recording_ids: RecordingID | Sequence[RecordingID],
        search_type: SearchType,
        use_siblings: bool = True,
        live: bool = False,
        year: int = None,
        cut_off: int = None
) -> Optional[MusicbrainzListResult]:
    if cut_off is None:
        cut_off = 97

    # get actual MB objects
    if isinstance(recording_ids, RecordingID):
        recordings = [get_recording(recording_ids)]
    else:
        recordings = [get_recording(x) for x in recording_ids]


    # also search for recording siblings
    if use_siblings:
        new_recordings = recordings.copy()
        for recording in recordings:
            for sibling in recording.siblings:
                if sibling not in new_recordings:
                    new_recordings.append(sibling)
        recordings = new_recordings

    recordings.sort()

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
        case SearchType.COMPILATION:
            search_field = "compilations"
        case SearchType.EXTENDED_ALBUM:
            search_field = "studio_albums"
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
        if artist.id == UNKNOWN_ARTIST_ID or artist.id == VA_ARTIST_ID:
            _logger.warning("Not iterating over unknown/va release groups")
            continue
        rgs = getattr(artist, search_field)
        rg: ReleaseGroup
        for rg in rgs:
            for recording in recordings:
                if search_type == SearchType.STUDIO_ALBUM:
                    if recording in rg.normal_recordings:
                        single_result = MusicbrainzSingleResult(release_group=rg, recording=recording)
                        if single_result not in found_rgs:
                            # _logger.debug(f"Found track {track.position}. {recording.artist_credit_phrase} - {recording.title}")
                            found_rgs.append(single_result)
                else:
                    if recording in rg.recordings:
                        single_result = MusicbrainzSingleResult(release_group=rg, recording=recording)
                        if single_result not in found_rgs:
                            #_logger.debug(f"Found track {track.position}. {recording.artist_credit_phrase} - {recording.title}")
                            found_rgs.append(single_result)
        if search_type == SearchType.STUDIO_ALBUM and live:
            _logger.debug("Searching for a live song: adding live albums in album search")
            for rg in getattr(artist, "live_albums"):
                for recording in recordings:
                    if recording in rg:
                        single_result = MusicbrainzSingleResult(release_group=rg, recording=recording)
                        if single_result not in found_rgs:
                            # _logger.debug(f"Found track {track.position}. {recording.artist_credit_phrase} - {recording.title}")
                            found_rgs.append(single_result)

    found_rgs.sort(live=live, year=year)
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


def search_by_recording(
        recordings: Recording | Sequence[Recording],
        use_siblings: bool = True,
        cut_off: int = None,
        live: bool = False,
        year: int = None,
        fallback_to_all: bool = False) -> MusicbrainzSearchResult:
    if isinstance(recordings, Recording):
        recordings = [recordings]

    recording_ids = [recording.id for recording in recordings]
    return search_by_recording_id(
        recording_ids=recording_ids,
        use_siblings=use_siblings,
        cut_off=cut_off,
        year=year,
        live=live,
        fallback_to_all=fallback_to_all)


def search_by_recording_id(
        recording_ids: RecordingID | Sequence[RecordingID],
        use_siblings: bool = True,
        cut_off: int = None,
        live: bool = False,
        year: int = None,
        fallback_to_all: bool = False

) -> MusicbrainzSearchResult:
    results: MusicbrainzSearchResult = MusicbrainzSearchResult(live=live, year=year)
    for search_type in SearchType:
        if search_type in [SearchType.CANONICAL, SearchType.ALL, SearchType.IMPORT, SearchType.MANUAL]:
            continue
        res = _search_release_group_by_recording_ids(
            recording_ids=recording_ids,
            search_type=SearchType(search_type),
            use_siblings=use_siblings,
            live=live,
            year=year,
            cut_off=cut_off)
        if res is not None:
            results.add_result(SearchType(search_type), res)

    if results.is_empty() and fallback_to_all:
        res = _search_release_group_by_recording_ids(
            recording_ids=recording_ids,
            search_type=SearchType.ALL,
            use_siblings=use_siblings,
            cut_off=cut_off)
        if res is not None:
            results.add_result(SearchType.ALL, res)

    return results


def recording_id_from_fingerprint(file: pathlib.Path, cut_off: int = None) -> list[RecordingID]:
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
        _logger.error("Could not obtain Acoustid fingerprint from webservice")
        _logger.error(f"status: {response}")
        return []

    recording_ids = []
    for result in response['results']:
        if result['score'] < cut_off / 100:
            continue
        _logger.debug(f"Processing acoustid https://acoustid.org/track/{result['id']}")
        if "recordings" not in result.keys():
            _logger.warning("Invalid response from acoustid server")
            continue

        potentials = [r for r in result["recordings"] if ('sources' in r.keys() and 'id' in r.keys())]
        if len(potentials) == 0:
            continue

        recordings = sorted(potentials, key=lambda x: x['sources'], reverse=True)
        rec = recordings[0]

        redirected_id = recording_redirect(rec['id'])
        recording = get_recording(redirected_id)

        if redirected_id not in recording_ids:
            _logger.debug(f"Acoustid: adding {recording} ({rec['sources']} sources)")
            recording_ids.append(redirected_id)

    return recording_ids


def search_fingerprint(file: pathlib.Path, cut_off: int = None) \
        -> MusicbrainzSearchResult:
    recording_ids = recording_id_from_fingerprint(file=file, cut_off=cut_off)

    recordings = [get_recording(x) for x in recording_ids]

    result = search_by_recording_id(recording_ids)

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
    recording_ids = recording_id_from_fingerprint(file=file, cut_off=cut_off)

    return _search_release_group_by_recording_ids(
        recording_ids=recording_ids,
        search_type=search_type,
        use_siblings=use_siblings,
        cut_off=cut_off
    )


def search_song(
        artist_query: str = None,
        title_query: str = None,
        file: pathlib.Path = None,
        year: int = None,
        seed_id: RecordingID = None,
        additional_seed_ids: Sequence[RecordingID] = None,
        fallback_to_all: bool = True,
        attempt_fast: bool = False,
        cut_off: int = None,
        hint_shortcut: bool = False
) -> Optional[MusicbrainzSearchResult]:
    if artist_query is None and title_query is None:
        if seed_id is None:
            raise IllegalArgumentError("Either provide artist_query and title_query, or a seed_id")
        seed_recording = get_recording(seed_id)
        _logger.warning("Reading artist_query and title_query from seed recording id")
        artist_query = seed_recording.artist_credit_phrase
        title_query = seed_recording.title

    hint = find_hint_recording(artist_query=artist_query, title_query=title_query)
    artist_query = hint["artist"]
    title_query = hint["title"]
    if "recording_id" in hint.keys():
        seed_recording = get_recording(hint["recording_id"])

        if hint_shortcut:
            _logger.info("Directly turning a hint into a result set.")
            hint_result = MusicbrainzSearchResult.result_from_recording(recording=seed_recording)
            if hint_result is not None and not hint_result.is_empty():
                return hint_result


    if cut_off is None:
        cut_off = 90

    live_title = title_is_live(title_query)

    _logger.info(f"Searching for '{artist_query}' - '{title_query}'")

    # First find a canonical result:
    _logger.info(f"Step 1. performing canonical search on '{artist_query}' - '{title_query}'")
    canonical: MusicbrainzListResult = search_song_canonical(artist_query=artist_query, title_query=title_query,
                                                             live=(live_title is not None), year=year)
    if not canonical and live_title:
        _logger.info(f"Step 1b. retry canonical search for live title on '{artist_query}' - '{live_title}'")
        canonical: MusicbrainzListResult = search_song_canonical(artist_query=artist_query, title_query=live_title,
                                                                 live=True, year=year)

    if canonical:
        _logger.info(f"Found canonical release: {canonical[0].track}")
        if attempt_fast:
            if (canonical[0].release_group.is_studio_album or
                    (live_title is not None and canonical[0].release_group.is_live_album)):
                _logger.debug(f"Happy with the canonical release that is an album, using that to bootstrap a result")
                return MusicbrainzSearchResult.result_from_recording(canonical[0].recording, canonical)

    # Doing fingerprint look up
    if file:
        _logger.info(f"Step 2. performing acoustid fingerprint lookup")
        songs_found_fp: list[Recording] = [get_recording(x) for x in recording_id_from_fingerprint(file)]
        _logger.debug(f"Found {len(songs_found_fp)} results from fingerprint lookup search")
    else:
        _logger.info(f"Step 2. skipping acoustid fingerprint lookup")
        songs_found_fp: list[Recording] = []

    # gathering potential recordings to start search from

    _logger.info(f"Step 3. performing a search query on Musicbrainz API")
    songs_found_mb: list[Recording] = search_song_musicbrainz(artist_query=artist_query, title_query=title_query,
                                                              cut_off=cut_off, strict=True)

    if len(songs_found_mb) < 5 and live_title:
        _logger.info(f"Step 3a. performing a search query on Musicbrainz API for live release")
        songs_found_mb += [r for r in search_song_musicbrainz(artist_query=artist_query, title_query=live_title,
                                                                  cut_off=cut_off, strict=True,
                                                                  secondary_type=ReleaseType.LIVE) if r not in songs_found_mb]

    if len(songs_found_mb) < 5 :
        _logger.info(f"Step 3b. performing a less restrictive search query on Musicbrainz API")
        songs_found_mb += [r for r in search_song_musicbrainz(artist_query=artist_query, title_query=title_query, cut_off=cut_off,
                                                 strict=False) if r not in songs_found_mb]

    if len(songs_found_mb) == 0:
        if canonical is not None:
            _logger.info(
                f"Step 3c. Bootstrapping search from canonical result {canonical[0].recording}.")
            songs_found_mb = [canonical[0].recording]
        else:
            _logger.info(
                f"Step 3d. Artist search to determine a different artist")
            artists: list[Artist] = search_artist_musicbrainz(artist_query=artist_query, cut_off=80)
            for artist in artists:
                songs_found_mb += [r for r in search_song_musicbrainz(artist_query=artist, title_query=title_query, cut_off=cut_off, strict=False) if r not in songs_found_mb]

    _logger.debug(f"Found {len(songs_found_mb)} from musicbrainz search")

    if not canonical and not songs_found_fp and not songs_found_mb:
        _logger.error(f"Could not determine potential recordings for '{artist_query}' - '{title_query}'")
        return None

    _logger.info(f"Step 4. analyzing results so far")

    #check crossovers
    fp_vs_mb: list[Recording] = [r for r in songs_found_fp if r in songs_found_mb]
    candidates: list[Recording]
    if fp_vs_mb:
        _logger.debug(f"Using results that appear both in fingerprinting as wel as search query")
        candidates = fp_vs_mb
    elif songs_found_fp:
        _logger.debug(f"Using fingerprinting results")
        candidates = songs_found_fp
    else:
        _logger.debug(f"Using results from search query")
        candidates = songs_found_mb

    if additional_seed_ids:
        _logger.debug(f"Adding additional seed ids")
        seed_recordings = [get_recording(x) for x in additional_seed_ids]
        new_candidates = seed_recordings + [c for c in candidates if c not in seed_recordings]
        candidates = new_candidates
    if seed_id:
        _logger.debug(f"Adding seed id")
        if seed_recording not in candidates:
            candidates = [seed_recording] + candidates

    _logger.info(f"Step 5. normalizing results")

    candidates.sort()

    reasons = set([r for c in candidates for r in c.performance_type])
    if PerformanceWorkAttributes.COVER in reasons and len(reasons) == 1:
        _logger.info("Assuming this work is a cover")
        do_all = True
    else:
        do_all = False

    if do_all:
        result: MusicbrainzSearchResult = search_by_recording(candidates, live=(live_title is not None),
                                                              fallback_to_all=fallback_to_all)

    else:
        candidates_normal = [c for c in candidates if c.is_normal_performance]
        candidates_other = [c for c in candidates if not c.is_normal_performance]

        _logger.debug(
            f"Found {len(candidates_normal)} normal performances and {len(candidates_other)} other performances")

        _logger.info(f"Step 6. Determining best releases")
        result_normal: MusicbrainzSearchResult = search_by_recording(candidates_normal, live=(live_title is not None), year=year,
                                                                     fallback_to_all=fallback_to_all)
        result_other: MusicbrainzSearchResult = search_by_recording(candidates_other, live=(live_title is not None), year=year,
                                                                    fallback_to_all=fallback_to_all)

        if result_normal.is_empty():
            result = result_other
        else:
            result = result_normal

    if canonical is not None:
        filter_canonical = MusicbrainzListResult()
        for r in canonical:
            # check whether release matches my expectations
            if r.release_group.is_va and not r.release_group.is_soundtrack:
                _logger.info(f"Canonical release for '{artist_query}' - '{title_query}' is VA and we don't want it")
                _logger.debug(f"Result was {r.release}")
            else:
                filter_canonical.append(r)
        if len(filter_canonical) > 0:
            result.add_result(SearchType.CANONICAL, filter_canonical)
        else:
            _logger.info("All canonical result were rejected...")

    elif not result.is_empty():
        _logger.info(f"Retrying failed canonical search using search result")
        k: SearchType
        v: MusicbrainzSingleResult
        for k, v in result.iterate_results():
            canonical = search_song_canonical(artist_query=v.recording.artist_credit_phrase,
                                              title_query=v.recording.title,
                                              live=v.recording.is_live)
            if canonical is not None:
                _logger.debug(f"Found canonical result via search for {k.name}")
                result.add_result(SearchType.CANONICAL, canonical)
                break

    if result.is_empty():
        _logger.warning(f"Could not find a match for '{artist_query}' - '{title_query}' after every search I tried")
        return None
    _logger.info(f"Found result {result}")
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
