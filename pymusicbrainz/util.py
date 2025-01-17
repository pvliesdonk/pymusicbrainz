import datetime
import logging
import re
from typing import Sequence, Optional, Any
from urllib.parse import urlencode

import rapidfuzz
import requests
import sqlalchemy as sa
import mbdata.models
from unidecode import unidecode

from .datatypes import RecordingID, ArtistID, ReleaseGroupID, ReleaseID, MBID, WorkID, PerformanceWorkAttributes, \
    TrackID
from .dataclasses import ReleaseGroup, Recording, Artist, Release, Work
from .exceptions import NotFoundError, MBIDNotExistsError
from .object_cache import get_artist, get_release_group, get_release, get_recording, get_work

_logger = logging.getLogger(__name__)

ARTIST_SPLITS = [  # \s? -> consume optional space, (?<!\w) before (?!\w) after
    r'\s?(?<!\w)&(?!\w)\s?',  # &
    r'\s?(?<!\w)\+(?!\w)\s?',  # +
    r'\s?(?<!\w),(?!\w)\s?',  # +
    r'\s?(?<!\w)ft\.?(?!\w)\s?',  # ft
    r'\s?(?<!\w)vs\.?(?!\w)\s?',  # vs
    r'\s?(?<!\w)featuring(?!\w)\s?',  # featuring
    r'\s?(?<!\w)feat\.?(?!\w)\s?',  # feat
    r'\s?(?<!\w)and(?!\w)\s?',  # and
    r'\s?(?<!\w)en(?!\w)\s?',  # en
    r'\s?(?<!\w)\(\s?'
]

SUBSTRING_SPLIT = r'(.*)[\[\(](.*?)[\[\)](.*)'
STRIP_CHARS = ' ().&'


def split_artist(s: str | Artist, include_first=True) -> list[str]:
    if isinstance(s, Artist):
        #nothing to split
        return [s.name]

    m = re.match(SUBSTRING_SPLIT, s)
    if m:
        split_results = [s] if include_first else []
        for t1 in m.groups():
            for t2 in split_artist(t1, include_first=False):
                if t2 and t2 not in split_results:
                    split_results.append(t2.strip(STRIP_CHARS))

        return split_results

    split_results = []
    for split_regex in ARTIST_SPLITS:
        split_result = re.split(split_regex, s, flags=re.IGNORECASE)
        if len(split_result) > 1:
            for r in split_result:
                if r not in split_results:
                    split_results.append(r.strip(STRIP_CHARS))

    if not split_results:
        return [s.strip(STRIP_CHARS)]
    else:
        recurse_result = [s] if include_first else []
        for r in split_results:
            t1 = split_artist(r)
            for t2 in t1:
                if t2 and t2 not in recurse_result:
                    recurse_result.append(t2)

        return recurse_result


def fold_sort_candidates(
        candidates: Sequence[tuple["ReleaseGroup", "Recording"]]) \
        -> list[tuple["ReleaseGroup", list["Recording"]]]:
    t1 = {}
    for (rg, recording) in candidates:
        if rg in t1.keys():
            t1[rg].append(recording)
        else:
            t1[rg] = [recording]

    t2 = sorted([(k, sorted(v)) for k, v in t1.items()], key=lambda x: x[0])
    return t2


def flatten_title(artist_name="", recording_name="", album_name="") -> str:
    """ Given the artist name and recording name, return a combined_lookup string """
    return unidecode(re.sub(r'\W+', '', artist_name + album_name + recording_name).lower())


def string_dif(s1: str, s2: str) -> float:
    rapidfuzz.fuzz.ratio(s1, s2, processor=rapidfuzz.utils.default_process
                         )


_re_live = re.compile(r'(.*) [(\[]live.*?[)\]].*?', re.IGNORECASE)
_re_unplugged = re.compile(r'(.*) [(\[]unplugged.*?[)\]].*?', re.IGNORECASE)
_re_live_at = re.compile(r'(.*) [(\[]live at.*?[)\]].*?', re.IGNORECASE)


def title_is_live(title: str) -> Optional[str]:
    m = _re_live.match(title)
    if m:
        new_title = m.group(1)
        return new_title
    if _re_unplugged.match(title):
        return title
    if _re_live_at.match(title):
        return title
    return None


def parse_partial_date(partial_date: mbdata.models.PartialDate) -> datetime.date | None:
    if partial_date.year is None:
        return None
    if partial_date.month is None:
        return datetime.date(year=partial_date.year, month=1, day=1)
    if partial_date.day is None:
        return datetime.date(year=partial_date.year, month=partial_date.month, day=1)
    return datetime.date(year=partial_date.year, month=partial_date.month, day=partial_date.day)


def area_to_country(area: mbdata.models.Area) -> Optional[str]:
    from pymusicbrainz import get_db_session
    with get_db_session() as session:
        if area is None:
            return None
        if area.type_id is not None and area.type_id != 1:
            stmt = (
                sa.select(mbdata.models.Area)
                .join_from(mbdata.models.AreaContainment, mbdata.models.Area, mbdata.models.AreaContainment.parent)
                .where(mbdata.models.AreaContainment.descendant_id == area.id)
                .where(mbdata.models.Area.type_id == 1)
            )
            parent_area = session.scalar(stmt)
            if parent_area is None:
                return None
            area = parent_area
        try:
            return area.iso_3166_1_codes[0].code
        except IndexError as ex:
            return None


def artist_redirect(artist_id: str | ArtistID) -> ArtistID:
    from pymusicbrainz import get_db_session

    if isinstance(artist_id, str):
        artist_id = ArtistID(artist_id)

    with get_db_session() as session:
        stmt = (
            sa.select(mbdata.models.Artist.gid)
            .join_from(mbdata.models.ArtistGIDRedirect, mbdata.models.Artist,
                       mbdata.models.ArtistGIDRedirect.artist)
            .where(mbdata.models.ArtistGIDRedirect.gid == str(artist_id))
        )
        res = session.scalar(stmt)
        if res is None:
            return artist_id
        else:
            return artist_redirect(str(res))


def release_group_redirect(rg_id: str | ReleaseGroupID) -> ReleaseGroupID:
    from pymusicbrainz import get_db_session

    if isinstance(rg_id, str):
        rg_id = ReleaseGroupID(rg_id)

    with get_db_session() as session:
        stmt = (
            sa.select(mbdata.models.ReleaseGroup.gid)
            .join_from(mbdata.models.ReleaseGroupGIDRedirect, mbdata.models.ReleaseGroup,
                       mbdata.models.ReleaseGroupGIDRedirect.release_group)
            .where(mbdata.models.ReleaseGroupGIDRedirect.gid == str(rg_id))
        )
        res = session.scalar(stmt)
        if res is None:
            return rg_id
        else:
            return release_group_redirect(str(res))


def release_redirect(release_id: str | ReleaseID) -> ReleaseID:
    from pymusicbrainz import get_db_session

    if isinstance(release_id, str):
        release_id = ReleaseID(release_id)

    with get_db_session() as session:
        stmt = (
            sa.select(mbdata.models.Release.gid)
            .join_from(mbdata.models.ReleaseGIDRedirect, mbdata.models.Release,
                       mbdata.models.ReleaseGIDRedirect.release)
            .where(mbdata.models.ReleaseGIDRedirect.gid == str(release_id))
        )
        res = session.scalar(stmt)
        if res is None:
            return release_id
        else:
            return release_redirect(str(res))


def recording_redirect(rec_id: str | RecordingID) -> RecordingID:
    from pymusicbrainz import get_db_session

    if isinstance(rec_id, str):
        rec_id = RecordingID(rec_id)

    with get_db_session() as session:
        stmt = (
            sa.select(mbdata.models.Recording.gid)
            .join_from(mbdata.models.RecordingGIDRedirect, mbdata.models.Recording,
                       mbdata.models.RecordingGIDRedirect.recording)
            .where(mbdata.models.RecordingGIDRedirect.gid == str(rec_id))
        )
        res = session.scalar(stmt)
        if res is None:
            return rec_id
        else:
            return recording_redirect(str(res))

def track_redirect(track_id: str | TrackID) -> TrackID:
    from pymusicbrainz import get_db_session

    if isinstance(track_id, str):
        track_id = TrackID(track_id)

    with get_db_session() as session:
        stmt = (
            sa.select(mbdata.models.Track.gid)
            .join_from(mbdata.models.TrackGIDRedirect, mbdata.models.Track,
                       mbdata.models.TrackGIDRedirect.track)
            .where(mbdata.models.TrackGIDRedirect.gid == str(track_id))
        )
        res = session.scalar(stmt)
        if res is None:
            return track_id
        else:
            return track_redirect(str(res))



_uuid_match = re.compile(r'[a-z0-9]{8}-?[a-z0-9]{4}-?[a-z0-9]{4}-?[a-z0-9]{4}-?[a-z0-9]{12}')
_url_match = re.compile(
    r'https?://.+?/(\w+)/([a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12})')


def id_from_string(id: str) -> MBID:
    # as url
    m = _url_match.match(id)
    if m:
        mbtype = m.group(1)
        mbid = m.group(2)
        _logger.debug(f"Identified '{id}' as a URL for objecttype '{mbtype}' with id '{mbid}'")

        match mbtype:
            case "artist":
                return artist_redirect(mbid)
            case "release-group":
                return release_group_redirect(mbid)
            case "release":
                return release_redirect(mbid)
            case "recording":
                return recording_redirect(mbid)
            case "work":
                return WorkID(mbid)
            case _:
                raise NotImplementedError

    # first try to parse as uuid
    if _uuid_match.match(id):
        _logger.debug(f"Identified {id} as a UUID. Determining type")
        try:
            result: Artist = get_artist(artist_redirect(id))
            return result.id
        except MBIDNotExistsError:
            pass
        try:
            result: ReleaseGroup = get_release_group(release_group_redirect(id))
            return result.id
        except MBIDNotExistsError:
            pass
        try:
            result: Release = get_release(release_redirect(id))
            return result.id
        except MBIDNotExistsError:
            pass
        try:
            result: Recording = get_recording(recording_redirect(id))
            return result.id
        except MBIDNotExistsError:
            pass
        try:
            result: Work = get_work(id)
            return result.id
        except MBIDNotExistsError:
            pass

    raise NotFoundError
