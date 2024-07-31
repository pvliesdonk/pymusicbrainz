import datetime
import logging
import re
from typing import Sequence

import mbdata.models
from unidecode import unidecode

from .dataclasses import ReleaseGroup, Recording

_logger = logging.getLogger(__name__)


def split_artist(artist_query: str) -> list[str]:
    result = [artist_query]

    splits = [" & ", " + ", " ft. ", " vs. ", " Ft. ", " feat. ", " and ", " en "]

    for split in splits:
        result = re.split(split, artist_query, flags=re.IGNORECASE)
        if len(result) > 1:
            for r in result:
                if r not in result:
                    result.append(r)

    return result


def fold_sort_candidates(
        candidates: Sequence[tuple["ReleaseGroup", "Recording"]])\
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
