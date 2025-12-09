# Extracting Core MusicBrainz Search Logic

This document describes the core search logic in `pymusicbrainz` for finding MusicBrainz release groups given an artist-title pair. This logic can be extracted for reuse in other projects.

## Overview

The library implements a **multi-stage fallback search strategy** with opinionated preferences for finding the "best" release group (album) for a given song. The main entry points are:

- `search.py:search_song()` - Full-featured search with all fallbacks
- `find.py:find_best_release_group()` - Simplified interface returning a single best result

## Core Search Flow

```
Artist + Title Query
       │
       ▼
┌──────────────────────┐
│ 1. Canonical Lookup  │  ◄── Typesense index of "canonical" releases
└──────────┬───────────┘
           │ (not found)
           ▼
┌──────────────────────┐
│ 2. Fingerprint Match │  ◄── AcoustID (optional, if file provided)
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│ 3. MusicBrainz API   │  ◄── musicbrainzngs search
│    Search            │
└──────────┬───────────┘
           │ (< 5 results)
           ▼
┌──────────────────────┐
│ 4. Fallback Searches │  ◄── Non-strict, artist-only, live variants
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│ 5. Merge & Validate  │  ◄── Sanity check + deduplication
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│ 6. Find Release      │  ◄── Priority: Studio Album > EP > Soundtrack > Single
│    Groups            │
└──────────────────────┘
```

## Extractable Core Components

### 1. String Normalization (`util.py:88-90`)

```python
from unidecode import unidecode
import re

def flatten_title(artist_name="", recording_name="", album_name="") -> str:
    """Normalize strings for comparison: lowercase, remove non-word chars, transliterate."""
    return unidecode(re.sub(r'\W+', '', artist_name + album_name + recording_name).lower())
```

**Purpose**: Creates a canonical string for fuzzy matching. Handles unicode (é→e), removes punctuation, lowercases.

### 2. Artist String Splitting (`util.py:38-71`)

```python
ARTIST_SPLITS = [
    r'\s?(?<!\w)&(?!\w)\s?',       # "Artist & Artist2"
    r'\s?(?<!\w)\+(?!\w)\s?',       # "Artist + Artist2"
    r'\s?(?<!\w),(?!\w)\s?',        # "Artist, Artist2"
    r'\s?(?<!\w)ft\.?(?!\w)\s?',    # "Artist ft. Artist2"
    r'\s?(?<!\w)vs\.?(?!\w)\s?',    # "Artist vs. Artist2"
    r'\s?(?<!\w)featuring(?!\w)\s?',
    r'\s?(?<!\w)feat\.?(?!\w)\s?',
    r'\s?(?<!\w)and(?!\w)\s?',
    r'\s?(?<!\w)en(?!\w)\s?',       # Dutch "en" = "and"
    r'\s?(?<!\w)\(\s?'              # Parenthetical artists
]

def split_artist(s: str) -> list[str]:
    """Split multi-artist strings into individual artist names."""
    # Recursively splits and returns all combinations
```

**Purpose**: Handles "Artist ft. Guest" or "Band & Solo Artist" queries by trying each component.

### 3. Live Title Detection (`util.py:98-112`)

```python
import re

_re_live = re.compile(r'(.*) [(\[]live.*?[)\]].*?', re.IGNORECASE)
_re_unplugged = re.compile(r'(.*) [(\[]unplugged.*?[)\]].*?', re.IGNORECASE)
_re_live_at = re.compile(r'(.*) [(\[]live at.*?[)\]].*?', re.IGNORECASE)

def title_is_live(title: str) -> str | None:
    """Extract base title if this is a live recording, e.g., 'Song (live)' → 'Song'"""
    m = _re_live.match(title)
    if m:
        return m.group(1)
    if _re_unplugged.match(title) or _re_live_at.match(title):
        return title  # Keep full title for venue-specific live recordings
    return None
```

### 4. Sanity Check / Match Validation

The `is_sane()` method validates that a result actually matches the query:

**For Recording** (`dataclasses.py:1279-1300`):
```python
def is_sane(self, artist_query: str, title_query: str, cut_off=70) -> bool:
    """Check if this recording reasonably matches the query."""
    # Check if any credited artist matches the query
    artist_sane = any([artist.is_sane(artist_query) for artist in self.artists])

    # Fuzzy match title against recording title + aliases
    title_ratio = rapidfuzz.process.extractOne(
        flatten_title(recording_name=title_query),
        [flatten_title(recording_name=self.title)] +
        [flatten_title(recording_name=a) for a in self.aliases],
        processor=rapidfuzz.utils.default_process
    )[1]

    return artist_sane and title_ratio >= cut_off
```

**For Artist** (`dataclasses.py:316-332`):
```python
def is_sane(self, artist_query: str, cut_off=70) -> bool:
    """Check if this artist matches the query."""
    artist_split = split_artist(artist_query)

    artist_ratios = [rapidfuzz.process.extractOne(
        flatten_title(artist_name=split),
        [flatten_title(self.name)] + [flatten_title(a) for a in self.aliases],
        processor=rapidfuzz.utils.default_process
    )[1] for split in artist_split]

    return max(artist_ratios) > cut_off
```

### 5. Fuzzy Matching

Uses `rapidfuzz` library (faster alternative to `fuzzywuzzy`):

```python
import rapidfuzz

# Weighted ratio for comparing two strings
score = rapidfuzz.fuzz.WRatio(
    query_normalized,
    candidate_normalized,
    processor=rapidfuzz.utils.default_process,
    score_cutoff=70
)

# Find best match from list of candidates
best_match, score, index = rapidfuzz.process.extractOne(
    query,
    candidates_list,
    processor=rapidfuzz.utils.default_process
)
```

### 6. Release Group Priority Logic (`find.py:383-422`)

```python
def select_best_candidate(candidates: dict) -> tuple[ReleaseGroup, Recording]:
    """
    Priority order:
    1. Studio Album (oldest first, unless soundtrack is older)
    2. EP (oldest first, unless soundtrack is older)
    3. Soundtrack
    4. Single

    Within each category: prefer oldest release date.
    """
    if len(candidates["studio_albums"]) > 0:
        if len(candidates["soundtracks"]) > 0:
            # If soundtrack is older than studio album, prefer soundtrack
            if candidates["soundtracks"][0] < candidates["studio_albums"][0]:
                return candidates["soundtracks"][0]
        return candidates["studio_albums"][0]
    elif len(candidates["eps"]) > 0:
        # Same logic for EPs
        ...
    elif len(candidates["soundtracks"]) > 0:
        return candidates["soundtracks"][0]
    elif len(candidates["singles"]) > 0:
        return candidates["singles"][0]
```

## Opinionated Decisions

1. **Cutoff Thresholds**:
   - MusicBrainz API search: 90% match
   - AcoustID fingerprint: 97% match
   - Fuzzy title/artist matching: 70% match
   - Artist search: 80-90% match

2. **Release Preferences**:
   - Prefers studio albums over soundtracks (unless soundtrack is older)
   - Excludes "Various Artists" releases (unless it's a soundtrack)
   - Filters out non-Latin scripts
   - Prefers official releases only
   - Sorts by release date (oldest = original release)

3. **Sibling Recording Expansion**: When a recording is found, also searches for "sibling" recordings (same work, same artist, different performance type like studio vs. live).

4. **Performance Type Handling**:
   - Normal performances → search standard album types
   - Covers → search ALL release types
   - Live/instrumental/etc → specific handling

5. **Multi-stage Fallback**:
   - Start with strict AND search
   - Fall back to non-strict (OR) search
   - Fall back to artist-only search then search by artist

## Dependencies

For the extractable logic:
- `rapidfuzz` - Fast fuzzy string matching
- `unidecode` - Unicode transliteration
- `musicbrainzngs` - MusicBrainz API client
- `acoustid` - Audio fingerprinting (optional)

For the full library:
- `sqlalchemy` + `mbdata` - Direct MusicBrainz database access
- `typesense` - Canonical release lookup

## Minimal Extractable Module

To reuse just the search logic without the database dependencies:

```python
# core_search.py - Minimal extractable search logic

import re
from unidecode import unidecode
import rapidfuzz
import musicbrainzngs

def flatten_title(artist_name="", recording_name="", album_name=""):
    return unidecode(re.sub(r'\W+', '', artist_name + album_name + recording_name).lower())

def is_match(query_artist, query_title, candidate_artist, candidate_title,
             candidate_aliases=None, cut_off=70):
    """Check if candidate matches query with fuzzy matching."""
    if candidate_aliases is None:
        candidate_aliases = []

    artist_score = rapidfuzz.fuzz.WRatio(
        flatten_title(artist_name=query_artist),
        flatten_title(artist_name=candidate_artist),
        processor=rapidfuzz.utils.default_process
    )

    title_candidates = [flatten_title(recording_name=candidate_title)]
    title_candidates += [flatten_title(recording_name=a) for a in candidate_aliases]

    title_match = rapidfuzz.process.extractOne(
        flatten_title(recording_name=query_title),
        title_candidates,
        processor=rapidfuzz.utils.default_process
    )
    title_score = title_match[1] if title_match else 0

    return artist_score >= cut_off and title_score >= cut_off

def search_musicbrainz(artist_query, title_query, cut_off=90, strict=True):
    """Search MusicBrainz API for recordings matching query."""
    results = []

    response = musicbrainzngs.search_recordings(
        query=artist_query,
        recording=title_query,
        status="official",
        video=False,
        strict=strict,
        limit=100
    )

    for r in response.get("recording-list", []):
        score = int(r.get("ext:score", 0))
        if score > cut_off:
            results.append({
                "id": r["id"],
                "title": r["title"],
                "artist": r.get("artist-credit-phrase", ""),
                "score": score
            })

    return results
```

## Files to Extract

| File | Key Functions | Purpose |
|------|---------------|---------|
| `util.py` | `flatten_title`, `split_artist`, `title_is_live` | String processing |
| `search.py` | `search_song_musicbrainz`, `search_artist_musicbrainz` | API search |
| `find.py` | `select_best_candidate` | Release group selection |
| `dataclasses.py` | `is_sane` methods | Match validation |

## Notes for Extraction

1. The database-dependent code (redirect resolution, canonical lookup, sibling finding) can be replaced with API-only alternatives if needed.

2. The Typesense canonical lookup is optional and can be skipped for simpler use cases.

3. The `is_sane()` validation is crucial for filtering out false positives from the MusicBrainz search.

4. Consider the sibling expansion logic if you want to find alternate versions of songs.
