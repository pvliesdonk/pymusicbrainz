import re
from typing import Sequence, Generator
from unidecode import unidecode
import datetime
import mbdata.models

def split_artist(artist_query: str) -> list[str]:
    l = [artist_query]

    splits = [" & ", " + ", " ft. ", " vs. ", " Ft. ", " feat. ", " and ", " en "]

    for split in splits:
        result = re.split(split, artist_query, flags=re.IGNORECASE)
        if len(result) > 1:
            for r in result:
                if r not in l:
                    l.append(r)
    #_logger.debug(f"Expanded artist query to: {l}")

    return l


def _fold_sort_candidates(candidates: Sequence[tuple["ReleaseGroup", "Recording"]]) -> list[
    tuple["ReleaseGroup", list["Recording"]]]:
    t1 = {}
    for (rg, recording) in candidates:
        if rg in t1.keys():
            t1[rg].append(recording)
        else:
            t1[rg] = [recording]

    t2 = sorted([(k, sorted(v)) for k, v in t1.items()], key=lambda x: x[0])
    return t2

def flatten_title(artist_name = "", recording_name = "", album_name = ""):
    """ Given the artist name and recording name, return a combined_lookup string """
    return unidecode(re.sub(r'[^\w]+', '', artist_name + album_name + recording_name).lower())


def parse_partial_date(pdate: mbdata.models.PartialDate) -> datetime.date | None:
    if pdate.year is None:
        return None
    if pdate.month is None:
        return datetime.date(year=pdate.year, month=1, day=1)
    if pdate.day is None:
        return datetime.date(year=pdate.year, month=pdate.month, day=1)
    return datetime.date(year=pdate.year, month=pdate.month,   day=pdate.day)


def cachedgenerator(func):
    def decorated(*args, **kwargs):
        generator = func(*args, **kwargs)
        return CachedGenerator(generator)

    return decorated


class CachedGenerator:
    def __init__(self, generator: Generator):
        self.generator: Generator = generator
        self.so_far: list = []
        self.exhausted: bool = False

    def __iter__(self):
        if self.exhausted:
            yield from self.so_far

        while True:
            try:
                next_item = next(self.generator)
                self.so_far.append(next_item)
                yield next_item

            except StopIteration:
                self.exhausted = True
                break