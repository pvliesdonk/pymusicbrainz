import re
from typing import Sequence

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
