import datetime
import logging
import re
from typing import Sequence, Optional

import sqlalchemy as sa
import mbdata.models
from unidecode import unidecode

from .datatypes import RecordingID, ArtistID, ReleaseGroupID, ReleaseID
from .dataclasses import ReleaseGroup, Recording

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


def split_artist(s: str, include_first=True) -> list[str]:
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

        return area.iso_3166_1_codes[0].code


def artist_redirect(rec_id: str | ArtistID) -> ArtistID:
    from pymusicbrainz import get_db_session

    if isinstance(rec_id, str):
        in_obj = ArtistID(rec_id)

    with get_db_session() as session:
        stmt = (
            sa.select(mbdata.models.Artist.gid)
            .join_from(mbdata.models.ArtistGIDRedirect, mbdata.models.Artist,
                       mbdata.models.ArtistGIDRedirect.artist)
            .where(mbdata.models.ArtistGIDRedirect.gid == str(rec_id))
        )
        res = session.scalar(stmt)
        if res is None:
            return rec_id
        else:
            return ArtistID(str(res))

def release_group_redirect(rec_id: str | ReleaseGroupID) -> ReleaseGroupID:
    from pymusicbrainz import get_db_session

    if isinstance(rec_id, str):
        in_obj = ReleaseGroupID(rec_id)

    with get_db_session() as session:
        stmt = (
            sa.select(mbdata.models.ReleaseGroup.gid)
            .join_from(mbdata.models.ReleaseGroupGIDRedirect, mbdata.models.ReleaseGroup,
                       mbdata.models.ReleaseGroupGIDRedirect.release_group)
            .where(mbdata.models.ReleaseGroupGIDRedirect.gid == str(rec_id))
        )
        res = session.scalar(stmt)
        if res is None:
            return rec_id
        else:
            return ReleaseGroupID(str(res))
        
def release_redirect(rec_id: str | ReleaseID) -> ReleaseID:
    from pymusicbrainz import get_db_session

    if isinstance(rec_id, str):
        in_obj = ReleaseID(rec_id)

    with get_db_session() as session:
        stmt = (
            sa.select(mbdata.models.Release.gid)
            .join_from(mbdata.models.ReleaseGIDRedirect, mbdata.models.Release,
                       mbdata.models.ReleaseGIDRedirect.release)
            .where(mbdata.models.ReleaseGIDRedirect.gid == str(rec_id))
        )
        res = session.scalar(stmt)
        if res is None:
            return rec_id
        else:
            return ReleaseID(str(res))

def recording_redirect(rec_id: str | RecordingID) -> RecordingID:
    from pymusicbrainz import get_db_session

    if isinstance(rec_id, str):
        in_obj = RecordingID(rec_id)

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
            return RecordingID(str(res))