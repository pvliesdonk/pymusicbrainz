import datetime
import enum
import logging
import pathlib
from typing import Mapping, Sequence

import acoustid
import rapidfuzz

from .api import MBApi
from .api import ACOUSTID_APIKEY
from .exceptions import NotFoundError, MBApiError
from .util import _fold_sort_candidates, flatten_title
from .datatypes import RecordingID
from .dataclasses import Artist, ReleaseGroup, Release, Recording, Work, Medium, Track, MusicBrainzObject

_logger = logging.getLogger(__name__)


class SearchType(enum.StrEnum):
    CANONICAL = "canonical"
    STUDIO_ALBUM = "studio_album"
    SINGLE = "single"
    SOUNDTRACK = "soundtrack"
    EP = "ep"
    ALL = "all"



def select_best_candidate(candidates: Mapping[str, Sequence[tuple[ReleaseGroup, Sequence[Recording]]]]) -> tuple[
    ReleaseGroup, Recording]:
    # {
    #     "studio_albums": sorted(albums),
    #     "eps": sorted(eps),
    #     "soundtracks": sorted(soundtracks),
    #     "singles": sorted(singles)
    # }

    if len(candidates["studio_albums"]) > 0:
        if len(candidates["soundtracks"]) > 0:
            if candidates["studio_albums"][0][0] < candidates["soundtracks"][0][0]:
                _logger.debug(f"Choosing oldest studio album over potential soundtrack")
                return candidates["studio_albums"][0][0], candidates["studio_albums"][0][1][0],
            else:
                _logger.debug(f"Choosing soundtrack that is older than oldest studio album")
                return candidates["soundtracks"][0][0], candidates["soundtracks"][0][1][0],
        else:
            _logger.debug(f"Choosing oldest studio album")
            return candidates["studio_albums"][0][0], candidates["studio_albums"][0][1][0],
    elif len(candidates["eps"]) > 0:
        if len(candidates["soundtracks"]) > 0:
            if candidates["eps"][0][0] < candidates["soundtracks"][0][0]:
                _logger.debug(f"Choosing oldest EP over potential soundtrack")
                return candidates["eps"][0][0], candidates["eps"][0][1][0],
            else:
                _logger.debug(f"Choosing soundtrack that is older than oldest EP")
                return candidates["soundtracks"][0][0], candidates["soundtracks"][0][1][0],
        else:
            _logger.debug(f"Choosing oldest EP")
            return candidates["eps"][0][0], candidates["eps"][0][1][0],
    elif len(candidates["soundtracks"]) > 0:
        _logger.debug(f"Choosing oldest soundtrack")
        return candidates["soundtracks"][0][0], candidates["soundtracks"][0][1][0],
    elif "singles" not in candidates.keys():
        raise NotFoundError("Expecting to look at singles, but they were not fetched.")
    elif len(candidates["singles"]) > 0:
        return candidates["singles"][0][0], candidates["singles"][0][1][0],
    else:
        raise NotFoundError("Somehow we didn't get any viable candidates")


def search_canonical_release(
        artist_query: str,
        title_query: str,
        mb_api: MBApi
) -> Mapping[str, MusicBrainzObject] | None:
    if mb_api is None:
        mb_api = MBApi()

    _logger.debug("Doing a lookup for canonical release")
    canonical_hits = mb_api.typesense_lookup(artist_query, title_query)
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
        mb_api: MBApi,
        search_type: SearchType,
        use_siblings: bool = True,
        cut_off: int = None
) -> Mapping[str, MusicBrainzObject] | None:
    if mb_api is None:
        mb_api = MBApi()

    if cut_off is None:
        cut_off = 97

    # get actual MB objects
    if isinstance(recording_ids, RecordingID):
        recordings = [mb_api.get_recording_by_id(recording_ids)]
    else:
        recordings = [mb_api.get_recording_by_id(x) for x in recording_ids]

    # also search for recording siblings
    if use_siblings:
        for recording in recordings:
            for sibling in recording.siblings:
                if sibling not in recordings:
                    recordings.append(sibling)

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
    found_rgs = []
    for recording in recordings:
        for artist in recording.artists:
            for rg in getattr(artist, search_field):
                if recording in rg:
                    track, release = find_track_release_for_release_group_recording(rg, recording)
                    if (rg, recording, release, track) not in found_rgs:
                        found_rgs.append((rg, recording, release, track))

    found_rgs = sorted(found_rgs, key=lambda x: x[2])
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
        mb_api: MBApi,
        use_siblings: bool = True,
        cut_off: int = None
) -> Mapping[str, MusicBrainzObject] | None:
    return _search_release_group_by_recording_ids(
        recording_ids=recording_ids,
        mb_api=mb_api,
        search_type=SearchType.STUDIO_ALBUM,
        use_siblings=use_siblings,
        cut_off=cut_off
    )


def search_soundtracks_by_recording_ids(
        recording_ids: RecordingID | Sequence[RecordingID],
        mb_api: MBApi,
        use_siblings: bool = True,
        cut_off: int = None
) -> Mapping[str, MusicBrainzObject] | None:
    return _search_release_group_by_recording_ids(
        recording_ids=recording_ids,
        mb_api=mb_api,
        search_type=SearchType.SOUNDTRACK,
        use_siblings=use_siblings,
        cut_off=cut_off
    )


def search_eps_by_recording_ids(
        recording_ids: RecordingID | Sequence[RecordingID],
        mb_api: MBApi,
        use_siblings: bool = True,
        cut_off: int = None
) -> Mapping[str, MusicBrainzObject] | None:
    return _search_release_group_by_recording_ids(
        recording_ids=recording_ids,
        mb_api=mb_api,
        search_type=SearchType.EP,
        use_siblings=use_siblings,
        cut_off=cut_off
    )


def search_singles_by_recording_ids(
        recording_ids: RecordingID | Sequence[RecordingID],
        mb_api: MBApi,
        use_siblings: bool = True,
        cut_off: int = None
) -> Mapping[str, MusicBrainzObject] | None:
    return _search_release_group_by_recording_ids(
        recording_ids=recording_ids,
        mb_api=mb_api,
        search_type=SearchType.SINGLE,
        use_siblings=use_siblings,
        cut_off=cut_off
    )


def search_release_groups_by_recording_ids(
        recording_ids: RecordingID | Sequence[RecordingID],
        mb_api: MBApi,
        use_siblings: bool = True,
        cut_off: int = None
) -> Mapping[str, MusicBrainzObject] | None:
    return _search_release_group_by_recording_ids(
        recording_ids=recording_ids,
        mb_api=mb_api,
        search_type=SearchType.ALL,
        use_siblings=use_siblings,
        cut_off=cut_off
    )


def search_by_recording_id(
        recording_ids: RecordingID | Sequence[RecordingID],
        mb_api: MBApi,
        use_siblings: bool = True,
        cut_off: int = None

) -> Mapping[SearchType, Mapping[str, MusicBrainzObject]]:
    results = {
        search_type: _search_release_group_by_recording_ids(
            recording_ids=recording_ids,
            mb_api=mb_api,
            search_type=SearchType(search_type),
            use_siblings=use_siblings,
            cut_off=cut_off) for search_type in SearchType}

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


def search_fingerprint(file: pathlib.Path, mb_api: MBApi, cut_off: int = None) \
        -> Mapping[SearchType, Mapping[str, MusicBrainzObject]]:
    if mb_api is None:
        mb_api = MBApi()

    recording_ids = _recording_id_from_fingerprint(file=file, cut_off=cut_off)
    return search_by_recording_id(recording_ids, mb_api=mb_api)


def search_fingerprint_by_type(
        file: pathlib.Path,
        search_type: SearchType,
        mb_api: MBApi,
        use_siblings: bool = True,
        cut_off: int = None) -> Mapping[str, MusicBrainzObject]:
    if mb_api is None:
        mb_api = MBApi()

    recording_ids = _recording_id_from_fingerprint(file=file, cut_off=cut_off)

    return _search_release_group_by_recording_ids(
        recording_ids=recording_ids,
        mb_api=mb_api,
        search_type=search_type,
        use_siblings=use_siblings,
        cut_off=cut_off
    )


def search_name(artist_query: str, title_query: str, mb_api: MBApi, cut_off: int = None) \
        -> Mapping[SearchType, Mapping[str, MusicBrainzObject]]:
    if mb_api is None:
        mb_api = MBApi()

    if cut_off is None:
        cut_off = 90

    # TODO: canonical lookup
    canonical = search_canonical_release(artist_query=artist_query, title_query=title_query, mb_api=mb_api)

    songs_found = mb_api.search_recording(artist_query=artist_query, title_query=title_query, cut_off=cut_off)
    recording_ids = [recording.id for recording in songs_found if recording.is_sane(artist_query, title_query)]
    result =  search_by_recording_id(recording_ids, mb_api=mb_api)
    result[SearchType.CANONICAL] = canonical

    return result

def search_name_by_type(
        artist_query: str,
        title_query: str,
        search_type: SearchType,
        mb_api: MBApi,
        use_siblings: bool = True,
        cut_off: int = None) -> Mapping[str, MusicBrainzObject]:
    if mb_api is None:
        mb_api = MBApi()

    songs_found = mb_api.search_recording(
        artist_query=artist_query,
        title_query=title_query,
        cut_off=cut_off)

    recording_ids = [recording.id for recording in songs_found if recording.is_sane(artist_query, title_query)]

    return _search_release_group_by_recording_ids(
        recording_ids=recording_ids,
        mb_api=mb_api,
        search_type=search_type,
        use_siblings=use_siblings,
        cut_off=cut_off
    )


def find_best_release_group(
        artist_query: str,
        title_query: str,
        mb_api: MBApi,
        canonical: bool = True,
        exhaustive: bool = True,
        lookup_singles: bool = True,

        date: int | datetime.date = None,
        file: pathlib.Path = None,
        cut_off: int = 90,
        search_cache: bool = True,
) -> tuple[ReleaseGroup, Recording, Release, Track] | tuple[None, None, None, None]:
    if mb_api is None:
        mb_api = MBApi()

    try:

        if isinstance(date, int):
            date = datetime.date(date, 1, 1)

        if canonical:
            _logger.debug("Doing a lookup for canonical release")
            canonical_hits = mb_api.typesense_lookup(artist_query, title_query)
            if len(canonical_hits) > 0:
                _logger.info("Found canonical release according to MusicBrainz Canonical dataset")
                rg: ReleaseGroup = canonical_hits[0]['release_group']
                recording: Recording = canonical_hits[0]['recording']
                release: Release = canonical_hits[0]['release']
                track: Track = find_track_for_release_recording(release, recording)
                return (rg, recording, release, track)
            else:
                _logger.info("No canonical release found. Falling back to brute force search")

        candidates = find_best_release_group_by_search(artist_query, title_query, date, cut_off,
                                                       lookup_singles=lookup_singles, mb_api=mb_api)

        if sum([len(x) for x in candidates.values()]) == 0:
            _logger.debug(f"Could not find a result by searching. Falling back to exhaustive artist search.")
            candidates = find_best_release_group_by_artist(artist_query, title_query, cut_off,
                                                           lookup_singles=lookup_singles, mb_api=mb_api)

        if sum([len(x) for x in candidates.values()]) == 0:
            _logger.debug(f"Could not find a result by exhaustive artist search. Trying Acoustid lookup")

            if file is not None:
                candidates = find_best_release_group_by_fingerprint(file, artist_query, title_query, cut_off,
                                                                    lookup_singles=lookup_singles, mb_api=mb_api)

        if sum([len(x) for x in candidates.values()]) == 0:
            _logger.debug(f"Could not find any candidates for this file")
            return None, None, None, None

        # we now have a dict with potential release groups
        rg: ReleaseGroup
        recording: Recording
        rg, recording = select_best_candidate(candidates)
        track, release = find_track_release_for_release_group_recording(rg, recording)

        return rg, recording, release, track

    except MBApiError as ex:
        _logger.exception(ex)
        return None, None, None, None


def find_release_for_release_group_recording(rg: ReleaseGroup, recording: Recording) -> Release:
    found = None
    for r in rg.releases:
        if recording in r:
            return r


def find_track_for_release_recording(release: Release, recording: Recording) -> Track:
    for track in release.tracks:
        if track.recording == recording:
            return track


def find_track_release_for_release_group_recording(rg: ReleaseGroup, recording: Recording) -> tuple[Track, Release]:
    for r in rg.releases:
        for track in r.tracks:
            if track.recording == recording:
                return track, r


def find_best_release_group_by_recording_ids(
        recording_ids: Sequence[RecordingID],

        artist_query: str = None,
        title_query: str = None,
        cut_off: int = 97,
        lookup_singles: bool = True,
        mb_api: MBApi = None
) -> Mapping[str, Sequence[tuple[ReleaseGroup, Sequence[Recording]]]]:
    if mb_api is None:
        mb_api = MBApi()

    result = []

    for recording_id in recording_ids:
        recording: Recording = mb_api.get_recording_by_id(recording_id)
        if artist_query is not None and title_query is not None:
            if recording.is_sane(artist_query=artist_query, title_query=title_query):
                result.append(recording)
        else:
            result.append(recording)

    albums = []
    eps = []
    soundtracks = []
    singles = []

    choice: ReleaseGroup
    for recording in result:
        for artist in recording.artists:
            for rg in artist.soundtracks:
                for recording2 in rg.recordings:
                    ratio = rapidfuzz.fuzz.WRatio(
                        flatten_title(recording.artist_credit_phrase, recording.title),
                        flatten_title(recording2.artist_credit_phrase, recording2.title),
                        processor=rapidfuzz.utils.default_process,
                        score_cutoff=cut_off
                    )
                    if ratio > cut_off and (rg, recording2) not in soundtracks:
                        soundtracks.append((rg, recording2))
                        _logger.debug(f"{ratio}%: {recording2} ")
            for rg in artist.studio_albums:
                for recording2 in rg.recordings:
                    ratio = rapidfuzz.fuzz.WRatio(
                        flatten_title(recording.artist_credit_phrase, recording.title),
                        flatten_title(recording2.artist_credit_phrase, recording2.title),
                        processor=rapidfuzz.utils.default_process,
                        score_cutoff=cut_off
                    )
                    if ratio > cut_off and (rg, recording2) not in albums:
                        albums.append((rg, recording2))
                        _logger.debug(f"{ratio}%: {recording2} ")
            for rg in artist.eps:
                for recording2 in rg.recordings:
                    ratio = rapidfuzz.fuzz.WRatio(
                        flatten_title(recording.artist_credit_phrase, recording.title),
                        flatten_title(recording2.artist_credit_phrase, recording2.title),
                        processor=rapidfuzz.utils.default_process,
                        score_cutoff=cut_off
                    )
                    if ratio > cut_off and (rg, recording2) not in eps:
                        eps.append((rg, recording2))
                        _logger.debug(f"{ratio}%: {recording2} ")

    # Do not load all singles if we don't need to
    if len(albums) + len(soundtracks) + len(eps) > 0:
        return {
            "studio_albums": _fold_sort_candidates(albums),
            "eps": _fold_sort_candidates(eps),
            "soundtracks": _fold_sort_candidates(soundtracks)
        }

    if not lookup_singles:
        _logger.warning(f"no album results, and --no-singles was provided")
        return {}

    _logger.debug(f"Did not find any albums; also looking at singles")
    for recording in result:
        for artist in recording.artists:
            for rg in artist.singles:
                for recording2 in rg.recordings:
                    ratio = rapidfuzz.fuzz.WRatio(
                        flatten_title(recording.artist_credit_phrase, recording.title),
                        flatten_title(recording2.artist_credit_phrase, recording2.title),
                        processor=rapidfuzz.utils.default_process,
                        score_cutoff=cut_off
                    )
                    if ratio > cut_off and (rg, recording2) not in singles:
                        singles.append((rg, recording2))
                        _logger.debug(f"{ratio}%: {recording2} ")

    return {
        "studio_albums": _fold_sort_candidates(albums),
        "eps": _fold_sort_candidates(eps),
        "soundtracks": _fold_sort_candidates(soundtracks),
        "singles": _fold_sort_candidates(singles)
    }


def find_best_release_group_by_fingerprint(
        file: pathlib.Path,

        artist_query: str = None,
        title_query: str = None,
        cut_off: int = None,
        lookup_singles: bool = True,
        mb_api: MBApi = None) -> Mapping[str, Sequence[tuple[ReleaseGroup, Sequence[Recording]]]]:
    _logger.debug(f"Scanning Acoustid fingerprint for file {file}")

    if mb_api is None:
        mb_api = MBApi()

    recording_ids = []
    try:
        for score, recording_id, title, artist in acoustid.match(ACOUSTID_APIKEY, str(file)):
            if score > (cut_off if cut_off is not None else 97) / 100:
                recording_ids.append(recording_id)
    except acoustid.FingerprintGenerationError as ex:
        _logger.error(f"Could not compute fingerprint for file '{file}'")
        raise MBApiError(f"Could not compute fingerprint for file '{file}'") from ex
    except acoustid.WebServiceError as ex:
        _logger.error("Could not obtain Acoustid fingerprint from webservice")
        raise MBApiError("Could not obtain Acoustid fingerprint from webservice") from ex

    return find_best_release_group_by_recording_ids(
        recording_ids,
        artist_query,
        title_query,
        cut_off,
        lookup_singles,
        mb_api=mb_api
    )


def find_best_release_group_by_artist(
        artist_query: str,

        title_query: str,
        year: int = None,
        cut_off: int = 90,
        lookup_singles: bool = True,
        mb_api: MBApi = None,
) -> Mapping[str, Sequence[tuple[ReleaseGroup, Sequence[Recording]]]]:
    artists_found = mb_api.search_artists(artist_query, cut_off=cut_off)

    if len(artists_found) == 0:
        _logger.debug(f"Could not identify potential artists via artist search")
        return {}

    _logger.info("Identified the following potential artists: \n" + "\n".join([str(a) for a in artists_found]))

    artist: Artist
    recording: Recording
    me = flatten_title(artist_query, title_query)

    them = {}
    for artist in artists_found:
        for rg in artist.soundtracks:
            for recording in rg.recordings:
                if recording.is_sane(artist_query=artist_query, title_query=title_query) and (
                        rg, recording) not in them.keys():
                    them[(rg, recording)] = flatten_title(recording.artist_credit_phrase, recording.title)
    result = rapidfuzz.process.extract(me, them, limit=None, score_cutoff=90, processor=rapidfuzz.utils.default_process)
    soundtracks = [x[2] for x in result]

    them = {}
    for artist in artists_found:
        for rg in artist.studio_albums:
            for recording in rg.recordings:
                if recording.is_sane(artist_query=artist_query, title_query=title_query) and (
                        rg, recording) not in them.keys():
                    them[(rg, recording)] = flatten_title(recording.artist_credit_phrase, recording.title)
    result = rapidfuzz.process.extract(me, them, limit=None, score_cutoff=90, processor=rapidfuzz.utils.default_process)
    albums = [x[2] for x in result]

    them = {}
    for artist in artists_found:
        for rg in artist.eps:
            for recording in rg.recordings:
                if recording.is_sane(artist_query=artist_query, title_query=title_query) and (
                        rg, recording) not in them.keys():
                    them[(rg, recording)] = flatten_title(recording.artist_credit_phrase, recording.title)
    result = rapidfuzz.process.extract(me, them, limit=None, score_cutoff=90, processor=rapidfuzz.utils.default_process)
    eps = [x[2] for x in result]

    # Do not load all singles if we don't need to
    if len(albums) + len(soundtracks) + len(eps) > 0:
        return {
            "studio_albums": _fold_sort_candidates(albums),
            "eps": _fold_sort_candidates(eps),
            "soundtracks": _fold_sort_candidates(soundtracks)
        }

    if not lookup_singles:
        _logger.warning(f"no album results, and --no-singles was provided")
        return {}

    _logger.debug(f"Did not find any albums; also looking at singles")

    them = {}
    for artist in artists_found:
        for rg in artist.singles:
            for recording in rg.recordings:
                if recording.is_sane(artist_query=artist_query, title_query=title_query) and (
                        rg, recording) not in them.keys():
                    them[(rg, recording)] = flatten_title(recording.artist_credit_phrase, recording.title)
    result = rapidfuzz.process.extract(me, them, limit=None, score_cutoff=90, processor=rapidfuzz.utils.default_process)
    singles = [x[2] for x in result]

    return {
        "studio_albums": _fold_sort_candidates(albums),
        "eps": _fold_sort_candidates(eps),
        "soundtracks": _fold_sort_candidates(soundtracks),
        "singles": _fold_sort_candidates(singles)
    }


def find_best_release_group_by_search(
        artist_query: str,
        title_query: str,

        date: datetime.date = None,
        cut_off: int = None,
        lookup_singles: bool = True,
        mb_api: MBApi = None) -> (
        Mapping[str, Sequence[tuple[ReleaseGroup, Sequence[Recording]]]]
):
    if mb_api is None:
        mb_api = MBApi()

    # First do a lookup for the song via a search query
    songs_found = mb_api.search_recording(artist_query=artist_query, title_query=title_query, cut_off=cut_off,
                                          date=date)

    _logger.info(f"Found {len(songs_found)} recordings in search")
    if len(songs_found) == 0:
        return {}

    _logger.info(f"First {len(songs_found) if len(songs_found) < 5 else 5} recordings are\n" + "\n".join(
        str(x) for x in songs_found[0:10]))
    recording: Recording

    # expand with siblings
    songs_found = [id for x in songs_found for id in x.siblings]

    _logger.info(f"Found {len(songs_found)} sibling recordings")
    if len(songs_found) == 0:
        return {}

    albums = []
    eps = []
    soundtracks = []
    singles = []

    for recording in songs_found:
        for artist in recording.artists:
            for album in artist.studio_albums:
                if recording.is_sane(artist_query=artist_query,
                                     title_query=title_query) and recording in album and (
                        album, recording) not in albums:
                    albums.append((album, recording))
            for ep in artist.eps:
                if recording.is_sane(artist_query=artist_query,
                                     title_query=title_query) and recording in ep and (ep, recording) not in eps:
                    eps.append((ep, recording))
            for soundtrack in artist.soundtracks:
                if recording.is_sane(artist_query=artist_query,
                                     title_query=title_query) and recording in soundtrack and (
                        soundtrack, recording) not in soundtracks:
                    soundtracks.append((soundtrack, recording))

    _logger.info(f"Found {len(albums)} potential albums in search")
    _logger.info(f"Found {len(eps)}  potential eps in search")
    _logger.info(f"Found {len(soundtracks)} soundtracks in search")

    # Do not load all singles if we don't need to
    if len(albums) + len(soundtracks) + len(eps) > 0:
        return {
            "studio_albums": _fold_sort_candidates(albums),
            "eps": _fold_sort_candidates(eps),
            "soundtracks": _fold_sort_candidates(soundtracks)
        }

    if not lookup_singles:
        _logger.warning(f"no album results, and --no-singles was provided")
        return {}

    _logger.debug(f"Did not find any albums; also looking at singles")

    for recording in songs_found:
        for artist in recording.artists:
            for single in artist.singles:
                if recording.is_sane(artist_query=artist_query,
                                     title_query=title_query) and recording in single and (
                        single, recording) not in singles:
                    singles.append((single, recording))

    _logger.info(f"Found {len(singles)} potential singles in search")

    return {
        "studio_albums": _fold_sort_candidates(albums),
        "eps": _fold_sort_candidates(eps),
        "soundtracks": _fold_sort_candidates(soundtracks),
        "singles": _fold_sort_candidates(singles)
    }
