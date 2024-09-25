import json
import logging
import pathlib
import re

from unidecode import unidecode

from .object_cache import get_recording
from .datatypes import ArtistID, RecordingID

_logger = logging.getLogger(__name__)


def _unidecode(text: str):
    return unidecode(re.sub(r'\W+', '', text.lower()))


_hintfile: pathlib.Path = None

_artist_name_hints: dict[str, str] = {}
_title_name_hints: dict[str, str] = {}
_artist_id_hints: dict[str, ArtistID] = {}
_recording_name_hints: dict[tuple[str, str], tuple[str, str]] = {}
_recording_id_hints: dict[tuple[str, str], RecordingID] = {}

_artist_name_hints_unidecode: dict[str, str] = {}
_title_name_hints_unidecode: dict[str, str] = {}
_artist_id_hints_unidecode: dict[str, ArtistID] = {}
_recording_name_hints_unidecode: dict[tuple[str, str], tuple[str, str]] = {}
_recording_id_hints_unidecode: dict[tuple[str, str], RecordingID] = {}


def add_artist_name_hint(match_name: str, new_name: str):
    if match_name in _artist_name_hints.keys():
        _logger.debug(f"Overwriting hint for '{match_name}': '{_artist_name_hints[match_name]}' --> '{new_name}'")
    _artist_name_hints[match_name] = new_name
    _artist_name_hints_unidecode[_unidecode(match_name)] = new_name


def add_artist_id_hint(match_name: str, artist_id: ArtistID):
    if match_name in _artist_id_hints.keys():
        _logger.debug(
            f"Overwriting hint for '{match_name}': '{str(_artist_id_hints[match_name])}' --> '{str(artist_id)}'")
    _artist_id_hints[match_name] = artist_id
    _artist_id_hints_unidecode[_unidecode(match_name)] = artist_id


def add_title_name_hint(match_name: str, new_name: str):
    if match_name in _title_name_hints.keys():
        _logger.debug(f"Overwriting hint for '{match_name}': '{_title_name_hints[match_name]}' --> '{new_name}'")
    _title_name_hints[match_name] = new_name
    _title_name_hints_unidecode[_unidecode(match_name)] = new_name


def add_recording_name_hint(match_artist: str, match_title: str, new_artist: str, new_title: str):
    if (match_artist, match_title) in _recording_name_hints.keys():
        _logger.debug(
            f"Overwriting hint for '{(match_artist, match_title)}' : '{_recording_name_hints[(match_artist, match_title)]}' --> '{(new_artist, new_title)}'")
    _recording_name_hints[(match_artist, match_title)] = (new_artist, new_title)
    _recording_name_hints_unidecode[(_unidecode(match_artist), _unidecode(match_title))] = (new_artist, new_title)


def add_recording_id_hint(match_artist: str, match_title: str, recording_id: RecordingID):
    if (match_artist, match_title) in _recording_id_hints.keys():
        _logger.debug(
            f"Overwriting hint for '{(match_artist, match_title)}' : '{str(_recording_id_hints[(match_artist, match_title)])}' --> '{str(recording_id)}")
    _recording_id_hints[(match_artist, match_title)] = recording_id
    _recording_id_hints_unidecode[(_unidecode(match_artist), _unidecode(match_title))] = recording_id


def configure_hintfile(hintfile: pathlib.Path) -> None:
    global _hintfile
    _hintfile = hintfile.resolve()

    if not _hintfile.exists():
        _logger.warning(f"Hintfile {hintfile.resolve()} cannot be found. Not reading hints")
        return

    load_hints()


def load_hints() -> None:
    with open(_hintfile, 'rt') as f:
        json_dict = json.load(f)

    for hint in json_dict:
        match hint["type"]:
            case "artist_id":
                add_artist_id_hint(hint["match_artist"], ArtistID(hint["new_artist_id"]))
            case "artist_name":
                add_artist_name_hint(hint["match_artist"], hint["new_artist"])
            case "title_name":
                add_title_name_hint(hint["match_title"], hint["new_title"])
            case "recording_id":
                add_recording_id_hint(hint["match_artist"], hint["match_title"], RecordingID(hint["new_recording_id"]))
            case "recording_name":
                add_recording_name_hint(hint["match_artist"], hint["match_title"],hint["new_artist"], hint["new_title"])


def save_hints() -> None:
    if _hintfile is None:
        _logger.error("No hintfile has been configured")
        return

    all_hints = []

    for key, hint in _artist_id_hints.items():
        all_hints.append({"type": "artist_id", "match_artist": key, "new_artist_id": str(hint)})

    for key, hint in _artist_name_hints.items():
        all_hints.append({"type": "artist_name", "match_artist": key, "new_artist": str(hint)})

    for key, hint in _title_name_hints.items():
        all_hints.append({"type": "title_name", "match_title": key, "new_title": str(hint)})

    for key, hint in _recording_name_hints.items():
        all_hints.append(
            {"type": "recording_name", "match_artist": key[0], "match_title": key[1], "new_artist": hint[0],
             "new_title": hint[1]})

    for key, hint in _artist_id_hints.items():
        all_hints.append(
            {"type": "recording_id", "match_artist": key[0], "match_title": key[1], "new_recording_id": str(hint)})

    with open(_hintfile, 'wt') as f:
        json.dump(all_hints, f, indent=2)




def find_hint_recording(artist_query: str, title_query: str) -> dict:
    unidecode_artist = _unidecode(artist_query)
    unidecode_title = _unidecode(title_query)

    result = {"artist": artist_query, "title": title_query}

    if (unidecode_artist,unidecode_title) in _recording_id_hints_unidecode.keys():
        result["recording_id"] = _recording_id_hints_unidecode[(unidecode_artist,unidecode_title)]
        recording = get_recording(result["recording_id"])
        result["artist"] = recording.artist_credit_phrase
        result["title"] = recording.title

    else:
        if unidecode_artist in _artist_name_hints_unidecode.keys():
            result["artist"] = _artist_name_hints_unidecode[unidecode_artist]

        if unidecode_title in _title_name_hints_unidecode.keys():
            result["title"] = _title_name_hints_unidecode[unidecode_title]

        if (unidecode_artist, unidecode_title) in _recording_name_hints_unidecode.keys():
            result["artist"], result["title"] = _recording_name_hints_unidecode[(unidecode_artist,unidecode_title)]

    if (result["title"] != title_query) or (result["artist"] != artist_query) or ("artist_id" in result.keys()) or ("recording_id" in result.keys()):
        _logger.debug(f"Found hint for '{artist_query}' - '{title_query}' --> {result}")
    return result