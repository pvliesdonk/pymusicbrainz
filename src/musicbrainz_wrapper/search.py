import datetime
import logging
import pathlib
from typing import Mapping, Sequence

import acoustid
import rapidfuzz

from .api import MBApi
from .api import ACOUSTID_APIKEY
from .exceptions import NotFoundError, MBApiError
from .util import _fold_sort_candidates
from .datatypes import RecordingID
from .dataclasses import Artist, ReleaseGroup, Release, Recording, Work, Medium, Track

_logger = logging.getLogger(__name__)


def select_best_candidate(candidates: Mapping[str, Sequence[tuple[ReleaseGroup, Sequence[Recording]]]]) -> tuple[ReleaseGroup, Recording]:
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


def find_best_release_group(
        artist_query: str,
        title_query: str,
        mb_api: MBApi,
        date: int | datetime.date = None,
        file: pathlib.Path = None,
        cut_off: int = 90,
        lookup_singles: bool = True,
        search_cache: bool = True,
        ) -> tuple[ReleaseGroup, Recording, Release, Track] | tuple[None, None, None, None]:

    if mb_api is None:
        mb_api = MBApi()

    try:

        if isinstance(date, int):
            date = datetime.date(date, 1, 1)

        candidates = find_best_release_group_by_search(artist_query, title_query, date, cut_off,
                                                       lookup_singles=lookup_singles,mb_api=mb_api)

        if sum([len(x) for x in candidates.values()]) == 0:
            _logger.debug(f"Could not find a result by searching. Falling back to exhaustive artist search.")
            candidates = find_best_release_group_by_artist(artist_query, title_query, cut_off,
                                                           lookup_singles=lookup_singles,  mb_api=mb_api)

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
                        f"{recording.artist_credit_phrase} - {recording.title}",
                        f"{recording2.artist_credit_phrase} - {recording2.title}",
                        processor=rapidfuzz.utils.default_process,
                        score_cutoff=cut_off
                    )
                    if ratio > cut_off and (rg, recording2) not in soundtracks:
                        soundtracks.append((rg, recording2))
                        _logger.debug(f"{ratio}%: {recording2} ")
            for rg in artist.studio_albums:
                for recording2 in rg.recordings:
                    ratio = rapidfuzz.fuzz.WRatio(
                        f"{recording.artist_credit_phrase} - {recording.title}",
                        f"{recording2.artist_credit_phrase} - {recording2.title}",
                        processor=rapidfuzz.utils.default_process,
                        score_cutoff=cut_off
                    )
                    if ratio > cut_off and (rg, recording2) not in albums:
                        albums.append((rg, recording2))
                        _logger.debug(f"{ratio}%: {recording2} ")
            for rg in artist.eps:
                for recording2 in rg.recordings:
                    ratio = rapidfuzz.fuzz.WRatio(
                        f"{recording.artist_credit_phrase} - {recording.title}",
                        f"{recording2.artist_credit_phrase} - {recording2.title}",
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
                        f"{recording.artist_credit_phrase} - {recording.title}",
                        f"{recording2.artist_credit_phrase} - {recording2.title}",
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
    me = f"{artist_query} - {title_query}"

    them = {}
    for artist in artists_found:
        for rg in artist.soundtracks:
            for recording in rg.recordings:
                if recording.is_sane(artist_query=artist_query, title_query=title_query) and (
                        rg, recording) not in them.keys():
                    them[(rg, recording)] = f"{recording.artist_credit_phrase} - {recording.title}"
    result = rapidfuzz.process.extract(me, them, limit=None, score_cutoff=90, processor=rapidfuzz.utils.default_process)
    soundtracks = [x[2] for x in result]

    them = {}
    for artist in artists_found:
        for rg in artist.studio_albums:
            for recording in rg.recordings:
                if recording.is_sane(artist_query=artist_query, title_query=title_query) and (
                        rg, recording) not in them.keys():
                    them[(rg, recording)] = f"{recording.artist_credit_phrase} - {recording.title}"
    result = rapidfuzz.process.extract(me, them, limit=None, score_cutoff=90, processor=rapidfuzz.utils.default_process)
    albums = [x[2] for x in result]

    them = {}
    for artist in artists_found:
        for rg in artist.eps:
            for recording in rg.recordings:
                if recording.is_sane(artist_query=artist_query, title_query=title_query) and (
                        rg, recording) not in them.keys():
                    them[(rg, recording)] = f"{recording.artist_credit_phrase} - {recording.title}"
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
                    them[(rg, recording)] = f"{recording.artist_credit_phrase} - {recording.title}"
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
