import datetime
from functools import cached_property
from typing import Union

import dateutil.parser
import rapidfuzz

from .datatypes import ArtistID, ReleaseGroupID, ReleaseType, ReleaseID, RecordingID, ReleaseStatus, WorkID, TrackID
from .exceptions import MBApiError, IncomparableError
from .util import split_artist, flatten_title
from .api import MBApi

import logging

_logger = logging.getLogger(__name__)


class Artist:

    def __init__(self, json: dict,
                 search_cache: bool = True,
                 fetch_cache: bool = True):
        self._json = json['artist']
        self._id: ArtistID = self._json['id']
        self._mb_api: MBApi = MBApi(search_cache=search_cache, fetch_cache=fetch_cache)

    @cached_property
    def id(self) -> ArtistID:
        return self._id

    @cached_property
    def name(self) -> str:
        return self._json['name']

    @cached_property
    def aliases(self) -> list[str]:
        if 'alias-list' in self._json:
            return [x['alias'] for x in self._json['alias-list']]
        return [self.name]

    @cached_property
    def artist_type(self) -> str | None:
        return self._json['type'] if 'type' in self._json.keys() else None

    @cached_property
    def sort_name(self) -> str:
        return self._json['sort-name'] if 'sort-name' in self._json.keys() else self.name

    @cached_property
    def disambiguation(self) -> str | None:
        return self._json["disambiguation"] if "disambiguation" in self._json.keys() else None

    @cached_property
    def country(self) -> str | None:
        return self._json["country"] if "country" in self._json.keys() else None

    @cached_property
    def url(self) -> str:
        return f"https://musicbrainz.org/artist/{self.id}"


    @cached_property
    def _release_group_result(self) -> list[dict]:
        return self._mb_api._fetch_release_groups_by_artist_id(self.id)

    @cached_property
    def release_group_ids(self) -> list[ReleaseGroupID]:
        _logger.debug(f"Browsing release groups for artist {self.name} [{self.id}]")
        return self._mb_api.get_release_group_ids_by_artist_id(self.id)

    @cached_property
    def release_groups(self) -> list["ReleaseGroup"]:
        _logger.debug(f"Fetching {len(self.release_group_ids)} release groups for artist {self.name} [{self.id}]")
        return [self._mb_api.get_release_group_by_id(x) for x in self.release_group_ids]

    @cached_property
    def album_ids(self) -> list[ReleaseGroupID]:
        result = []
        for rg in self._release_group_result:
            if "primary-type" in rg.keys() and ReleaseType(rg["primary-type"]) == ReleaseType.ALBUM:
                result.append(ReleaseGroupID(rg['id']))
        return result

    @cached_property
    def albums(self) -> list["ReleaseGroup"]:
        _logger.debug(f"Fetching {len(self.album_ids)} release groups for artist {self.name} [{self.id}] (albums)")
        return [self._mb_api.get_release_group_by_id(x) for x in self.album_ids]

    @cached_property
    def single_ids(self) -> list[ReleaseGroupID]:
        result = []
        for rg in self._release_group_result:
            if "primary-type" in rg.keys() and ReleaseType(rg["primary-type"]) == ReleaseType.SINGLE:
                result.append(ReleaseGroupID(rg['id']))
        return result

    @cached_property
    def singles(self) -> list["ReleaseGroup"]:
        _logger.debug(f"Fetching {len(self.single_ids)} release groups for artist {self.name} [{self.id}] (singles)")
        return [self._mb_api.get_release_group_by_id(x) for x in self.single_ids]

    @cached_property
    def ep_ids(self) -> list[ReleaseGroupID]:
        result = []
        for rg in self._release_group_result:
            if "primary-type" in rg.keys() and ReleaseType(rg["primary-type"]) == ReleaseType.EP:
                result.append(ReleaseGroupID(rg['id']))
        return result

    @cached_property
    def eps(self) -> list["ReleaseGroup"]:
        _logger.debug(f"Fetching {len(self.ep_ids)} release groups for artist {self.name} [{self.id}] (EPs)")
        return [self._mb_api.get_release_group_by_id(x) for x in self.ep_ids]

    @cached_property
    def studio_album_ids(self) -> list[ReleaseGroupID]:
        result = []
        for rg in self._release_group_result:
            if "primary-type" in rg.keys() and ReleaseType(
                    rg["primary-type"]) == ReleaseType.ALBUM and "secondary-type-list" not in rg.keys():
                result.append(ReleaseGroupID(rg['id']))
        return result

    @cached_property
    def studio_albums(self) -> list["ReleaseGroup"]:
        _logger.debug(f"Fetching {len(self.studio_album_ids)} release groups for artist {self.name} [{self.id}] (studio albums)")
        return [self._mb_api.get_release_group_by_id(x) for x in self.studio_album_ids]

    @cached_property
    def soundtrack_ids(self) -> list[ReleaseGroupID]:
        result = []
        for rg in self._release_group_result:
            if "primary-type" in rg.keys() and ReleaseType(
                    rg["primary-type"]) == ReleaseType.ALBUM and "secondary-type-list" in rg.keys() and str(
                ReleaseType.SOUNDTRACK) in rg["secondary-type-list"]:
                result.append(ReleaseGroupID(rg['id']))
        return result

    @cached_property
    def soundtracks(self) -> list["ReleaseGroup"]:
        _logger.debug(f"Fetching {len(self.soundtrack_ids)} release groups for artist {self.name} [{self.id}] (soundtracks)")
        return [self._mb_api.get_release_group_by_id(x) for x in self.soundtrack_ids]

    @cached_property
    def live_albums(self) -> list["ReleaseGroup"]:
        #return [x for x in self.release_groups if (x.primary_type == ReleaseType.ALBUM and ReleaseType.LIVE in x.types)]
        return [x for x in self.albums if (ReleaseType.LIVE in x.types)]

    @cached_property
    def remix_albums(self) -> list["ReleaseGroup"]:
        # return [x for x in self.release_groups if (x.primary_type == ReleaseType.ALBUM and ReleaseType.REMIX in x.types)]
        return [x for x in self.albums if (ReleaseType.REMIX in x.types)]

    @cached_property
    def compilations(self) -> list["ReleaseGroup"]:
        # return [x for x in self.release_groups if (x.primary_type == ReleaseType.ALBUM and ReleaseType.COMPILATION in x.types)]
        return [x for x in self.albums if
                (ReleaseType.COMPILATION in x.types)]

    @cached_property
    def release_ids(self) -> list[ReleaseID]:
        _logger.debug(f"Browsing all releases for artist {self.name} [{self.id}] ")
        return self._mb_api.get_release_ids_by_artist_id(self.id)

    @cached_property
    def releases(self) -> list["Release"]:
        _logger.debug(f"Fetching {len(self.release_ids)} releases for artist {self.name} [{self.id}]")
        return [self._mb_api.get_release_by_id(x) for x in self.release_ids]

    @cached_property
    def recording_ids(self) -> list[RecordingID]:
        _logger.debug(f"Browsing all recordings for artist {self.name} [{self.id}]")
        return self._mb_api.get_recording_ids_by_artist_id(self.id)

    @cached_property
    def recordings(self) -> list["Recording"]:
        _logger.debug(f"Fetching {len(self.recording_ids)} for artist {self.name} [{self.id}]")
        return [self._mb_api.get_recording_by_id(x) for x in self.recording_ids]

    def is_sane(self, artist_query: str, cut_off=70) -> bool:

        artist_split = split_artist(artist_query)

        artist_ratios = [rapidfuzz.process.extractOne(
            flatten_title(artist_name=split),
            [flatten_title(self.name)] + [flatten_title(a) for a in self.aliases],
            processor=rapidfuzz.utils.default_process
        )[1] for split in artist_split]
        artist_ratio = max(artist_ratios)
        if artist_ratio < cut_off:
            _logger.debug(f"{self} is not a sane candidate for artist {artist_query}")
        return artist_ratio > cut_off

    def __repr__(self):
        if self.disambiguation is not None:
            return f"Artist:  {self.name} [{self.id}] ({self.disambiguation})"
        else:
            return f"Artist:  {self.name} [{self.id}]"

    def __eq__(self, other):
        if isinstance(other, Artist):
            return self.id == other.id
        else:
            return False

    def __lt__(self, other):
        return self.sort_name < other.sort_name

    def __contains__(self, item):
        if isinstance(item, Release):
            return any([release_id == item.id for release_id in self.release_ids])
        if isinstance(item, ReleaseID):
            return any([release_id == item for release_id in self.release_ids])
        if isinstance(item, ReleaseGroup):
            return any([rg_id == item.id for rg_id in self.release_group_ids])
        if isinstance(item, ReleaseGroupID):
            return any([rg_id == item for rg_id in self.release_group_ids])
        if isinstance(item, Recording):
            return any([recording_id == item.id for recording_id in self.recording_ids])
        if isinstance(item, RecordingID):
            return any([recording_id == item for recording_id in self.recording_ids])

    def __hash__(self):
        return hash(self.id)


class ReleaseGroup:

    def __init__(self, json: dict,
                 search_cache: bool = True,
                 fetch_cache: bool = True):
        self._json = json['release-group']
        self._id: ReleaseGroupID = self._json['id']
        self._mb_api: MBApi = MBApi(search_cache=search_cache, fetch_cache=fetch_cache)

    @cached_property
    def id(self) -> ReleaseGroupID:
        return self._id

    @cached_property
    def title(self) -> str:
        return self._json["title"]

    @cached_property
    def aliases(self) -> list[str]:
        if 'alias-list' in self._json:
            return [x['alias'] for x in self._json['alias-list']]
        return [self.title]

    @cached_property
    def url(self) -> str:
        return f"https://musicbrainz.org/release-group/{self.id}"

    @cached_property
    def primary_type(self) -> ReleaseType | None:
        return ReleaseType(self._json["primary-type"]) if "primary-type" in self._json.keys() else None

    @cached_property
    def types(self) -> list[ReleaseType]:
        types = []
        if self.primary_type is not None:
            types.append(self.primary_type)
        if "secondary-type-list" in self._json.keys():
            for t in self._json["secondary-type-list"]:
                types.append(ReleaseType(t))
        return types

    @cached_property
    def is_studio_album(self) -> bool:
        return self.primary_type == ReleaseType.ALBUM and len(self.types) == 1

    @cached_property
    def is_single(self) -> bool:
        return self.primary_type == ReleaseType.SINGLE

    @cached_property
    def is_soundtrack(self) -> bool:
        return self.primary_type == ReleaseType.ALBUM and ReleaseType.SOUNDTRACK in self.types

    @cached_property
    def is_compilation(self) -> bool:
        return ReleaseType.COMPILATION in self.types

    @cached_property
    def is_eps(self) -> bool:
        return self.primary_type == ReleaseType.EP

    @cached_property
    def artist_credit_phrase(self) -> str:

        if "artist-credit-phrase" in self._json.keys():
            return self._json["artist-credit-phrase"]
        elif ("artist-credit" in self._json.keys()) and (
                len(self._json["artist-credit"]) == 1
        ):
            return self._json["artist-credit"][0]["artist"]["name"]
        else:
            raise MBApiError("Could not determine artistcredit phrase")

    @cached_property
    def status(self) -> ReleaseStatus | None:
        return ReleaseStatus(self._json["status"]) if "status" in self._json.keys() else None

    @cached_property
    def first_release_date(self) -> datetime.date | None:
        try:
            return dateutil.parser.parse(self._json["first-release-date"]).date() \
                if "first-release-date" in self._json.keys() else None

        except dateutil.parser.ParserError:
            return None

    @cached_property
    def artist_ids(self) -> list[ArtistID]:
        ids = []
        for a in self._json['artist-credit']:
            if isinstance(a, dict):
                ids.append(ArtistID(a['artist']['id']))
        return ids

    @cached_property
    def artists(self) -> list[Artist]:
        return [self._mb_api.get_artist_by_id(x) for x in self.artist_ids]

    @cached_property
    def release_ids(self) -> list[ReleaseID]:

        return self._mb_api.get_release_ids_by_release_group_id(self.id)

    @cached_property
    def releases(self) -> list["Release"]:
        _logger.debug(f"Fetching {len(self.release_ids)} releases for release group '{self.artist_credit_phrase}'- '{self.title}' [{self.id}]")
        return [self._mb_api.get_release_by_id(x) for x in self.release_ids]

    @cached_property
    def recording_ids(self) -> list[RecordingID]:
        result = []
        for release in self.releases:
            for recording_id in release.recording_ids:
                if recording_id not in result:
                    result.append(recording_id)
        return result

    @cached_property
    def recordings(self) -> list["Recording"]:
        _logger.debug(f"Fetching {len(self.recording_ids)} recordings for release group '{self.artist_credit_phrase}'- '{self.title}' [{self.id}]")
        return [self._mb_api.get_recording_by_id(x) for x in self.recording_ids]

    def is_sane(self, artist_query: str, title_query: str, cut_off=70) -> bool:
        artist_ratio = rapidfuzz.fuzz.WRatio(
            f"{self.artist_credit_phrase}",
            f"{artist_query}",
            processor=rapidfuzz.utils.default_process,
            score_cutoff=cut_off
        )
        if artist_ratio < cut_off:
            _logger.warning(f"{self} is not a sane candidate for artist {artist_query}")
        title_ratio = rapidfuzz.process.extractOne(
            f"{title_query}",
            [self.title]+self.aliases,
            processor=rapidfuzz.utils.default_process
        )[1]
        if title_ratio < cut_off:
            _logger.warning(f"{self} is not a sane candidate for title {title_query}")
        return artist_ratio > cut_off and title_ratio > cut_off

    def __repr__(self):
        s1 = f" [{self.primary_type}]" if self.primary_type is not None else ""
        s2 = (
            f" {self.first_release_date}" if self.first_release_date is not None else ""
        )
        return f"Release Group:  {self.artist_credit_phrase} - {self.title}{s1}{s2} [{self.id}]"

    def __eq__(self, other):
        if isinstance(other, ReleaseGroup):
            return self.id == other.id
        else:
            return False

    def __lt__(self, other):
        if isinstance(other, ReleaseGroup):

            if self.first_release_date is not None:
                if other.first_release_date is not None:
                    return self.first_release_date < other.first_release_date
                else:
                    return True
            else:
                return False

    def __contains__(self, item):
        if isinstance(item, Artist):
            return any([artist_id == item.id for artist_id in self.artist_ids])
        if isinstance(item, ArtistID):
            return any([artist_id == item for artist_id in self.artist_ids])
        if isinstance(item, Release):
            return any([release_id == item.id for release_id in self.release_ids])
        if isinstance(item, ReleaseID):
            return any([release_id == item for release_id in self.release_ids])
        if isinstance(item, Recording):
            return any([recording_id == item.id for recording_id in self.recording_ids])
        if isinstance(item, RecordingID):
            return any([recording_id == item for recording_id in self.recording_ids])

    def __hash__(self):
        return hash(self.id)


class Release:
    def __init__(self, json: dict,
                 search_cache: bool = True,
                 fetch_cache: bool = True):
        self._json = json['release']
        self._id: ReleaseID = self._json['id']
        self._mb_api: MBApi = MBApi(search_cache=search_cache, fetch_cache=fetch_cache)

    @cached_property
    def id(self) -> ReleaseID:
        return self._id

    @cached_property
    def title(self) -> str:
        return self._json["title"]

    @cached_property
    def aliases(self) -> list[str]:
        if 'alias-list' in self._json:
            return [x['alias'] for x in self._json['alias-list']]
        return [self.title]

    @cached_property
    def url(self) -> str:
        return f"https://musicbrainz.org/release/{self.id}"

    @cached_property
    def release_date(self) -> datetime.date | None:
        try:
            return dateutil.parser.parse(self._json["date"]).date() if "date" in self._json.keys() else None
        except dateutil.parser.ParserError:
            return None

    @cached_property
    def status(self) -> ReleaseStatus | None:
        return ReleaseStatus(self._json["status"]) if "status" in self._json.keys() else None

    @cached_property
    def country(self) -> str | None:
        return self._json["country"] if "country" in self._json.keys() else None

    @cached_property
    def release_group_id(self) -> ReleaseGroupID | None:
        return self._json["release-group"]["id"] if "release-group" in self._json.keys() else None

    @cached_property
    def release_group(self) -> ReleaseGroup | None:
        release_group_id = self.release_group_id
        if release_group_id is None:
            return None
        else:
            return self._mb_api.get_release_group_by_id(release_group_id)

    @cached_property
    def artist_credit_phrase(self) -> str:

        if "artist-credit-phrase" in self._json.keys():
            return self._json["artist-credit-phrase"]
        elif ("artist-credit" in self._json.keys()) and (
                len(self._json["artist-credit"]) == 1
        ):
            return self._json["artist-credit"][0]["artist"]["name"]
        else:
            raise MBApiError("Could not determine artistcredit phrase")

    @cached_property
    def artist_ids(self) -> list[ArtistID]:
        ids = []
        for a in self._json['artist-credit']:
            if isinstance(a, dict):
                ids.append(ArtistID(a['artist']['id']))
        return ids

    @cached_property
    def artists(self) -> list[Artist]:
        return [self._mb_api.get_artist_by_id(x) for x in self.artist_ids]

    @cached_property
    def media(self) -> list["Medium"]:
        return [Medium(self, m) for m in self._json['medium-list']]

    @cached_property
    def tracks(self) -> list["Track"]:
        l = []
        for m in self.media:
            for t in m.tracks:
                l.append(t)
        return l

    @cached_property
    def recording_ids(self) -> list[RecordingID]:
        ids = []
        for m in self.media:
            for t in m.tracks:
                ids.append(t.recording_id)
        return ids

    @cached_property
    def recordings(self) -> list["Recording"]:
        _logger.debug(f"Fetching {len(self.recording_ids)} recordings for release '{self.artist_credit_phrase}'- '{self.title}' [{self.id}]")
        return [self._mb_api.get_recording_by_id(x) for x in self.recording_ids]

    @cached_property
    def date(self) -> datetime.date | None:
        try:
            return dateutil.parser.parse(self._json["date"]).date() if "date" in self._json.keys() else None
        except dateutil.parser.ParserError:
            return None

    def is_sane(self, artist_query: str, title_query: str, cut_off=70) -> bool:
        artist_ratio = rapidfuzz.fuzz.WRatio(
            f"{self.artist_credit_phrase}",
            f"{artist_query}",
            processor=rapidfuzz.utils.default_process,
            score_cutoff=cut_off
        )
        if artist_ratio < cut_off:
            _logger.warning(f"{self} is not a sane candidate for artist {artist_query}")
        title_ratio = rapidfuzz.process.extractOne(
            f"{title_query}",
            [self.title]+self.aliases,
            processor=rapidfuzz.utils.default_process
        )[1]
        if title_ratio < cut_off:
            _logger.warning(f"{self} is not a sane candidate for title {title_query}")
        return artist_ratio > cut_off and title_ratio > cut_off

    def __repr__(self):
        return f"Release:  {self.artist_credit_phrase}: {self.title} [{self.status}/{self.id}]"

    def __eq__(self, other):
        if isinstance(other, Release):
            return self.id == other.id
        else:
            return False

    def __contains__(self, item):
        if isinstance(item, Artist):
            return any([artist_id == item.id for artist_id in self.artist_ids])
        if isinstance(item, ArtistID):
            return any([artist_id == item for artist_id in self.artist_ids])
        if isinstance(item, Recording):
            return any([recording_id == item.id for recording_id in self.recording_ids])
        if isinstance(item, RecordingID):
            return any([recording_id == item for recording_id in self.recording_ids])

    def __lt__(
            self,
            other):
        if isinstance(other, Release):
            if self.release_group != other.release_group:
                return self.release_group.first_release_date < other.release_group.first_release_date

            if self.date is not None and other.date is not None:
                return self.date < other.date
            else:
                return True
                #raise IncomparableError(self, other)
        return NotImplemented

    def __hash__(self):
        return hash(self.id)


class Recording:
    def __init__(self, json: dict,
                 search_cache: bool = True,
                 fetch_cache: bool = True):
        self._json = json['recording']
        self._id: RecordingID = self._json['id']
        self._mb_api: MBApi = MBApi(search_cache=search_cache, fetch_cache=fetch_cache)

    @cached_property
    def id(self) -> RecordingID:
        return self._id

    @cached_property
    def title(self) -> str:
        return self._json["title"]

    @cached_property
    def aliases(self) -> list[str]:
        if 'alias-list' in self._json:
            return [x['alias'] for x in self._json['alias-list']]
        return [self.title]

    @cached_property
    def length(self) -> int | None:
        return int(self._json["length"]) if "length" in self._json.keys() else None

    @cached_property
    def disambiguation(self) -> str | None:
        return self._json["disambiguation"] if "disambiguation" in self._json.keys() else None

    @cached_property
    def artist_credit_phrase(self) -> str:

        if "artist-credit-phrase" in self._json.keys():
            return self._json["artist-credit-phrase"]
        elif ("artist-credit" in self._json.keys()) and (
                len(self._json["artist-credit"]) == 1
        ):
            return self._json["artist-credit"][0]["artist"]["name"]
        else:
            raise MBApiError("Could not determine artistcredit phrase")

    @cached_property
    def artist_ids(self) -> list[ArtistID]:
        ids = []
        for a in self._json['artist-credit']:
            if isinstance(a, dict):
                ids.append(ArtistID(a['artist']['id']))
        return ids

    @cached_property
    def artists(self) -> list[Artist]:
        return [self._mb_api.get_artist_by_id(x) for x in self.artist_ids]

    @cached_property
    def performance_of_id(self) -> WorkID|None:
        if 'work-relation-list' not in self._json.keys():
            return None
        for a in self._json['work-relation-list']:
            if 'work' in a.keys() and a['direction'] == 'forward' and a['type'] == 'performance':
                return WorkID(a['work']['id'])

    @cached_property
    def performance_type(self) -> list[str]:
        l = []
        if 'work-relation-list' not in self._json.keys():
            return l

        for a in self._json['work-relation-list']:
            if 'work' in a.keys() and a['direction'] == 'forward' and a['type'] == 'performance':
                if 'attribute-list' in a.keys():
                    for att in a['attribute-list']:
                        if att not in l:
                            l.append(att)
        return l

    @cached_property
    def performance_of(self) -> Union["Work",None]:
        if self.performance_of_id is not None:
            return self._mb_api.get_work_by_id(self.performance_of_id)
        return None

    @cached_property
    def sibling_ids(self) -> list[RecordingID]:
        if self.performance_of is not None:
            if len(self.performance_type) == 0:
                return self.performance_of.performance_ids['no-attr']
            else:
                _logger.debug(f"Skipping sibling recordings because recording is a {'/'.join(self.performance_type)}")
        return []

    @cached_property
    def siblings(self) -> list["Recording"]:
        _logger.debug(f"Fetching {len(self.sibling_ids)} siblings for recording '{self.artist_credit_phrase}'- '{self.title}' [{self.id}]")
        return [self._mb_api.get_recording_by_id(x) for x in self.sibling_ids]

    @cached_property
    def first_release_date(self) -> datetime.date | None:
        try:
            return dateutil.parser.parse(self._json["first-release-date"]).date() \
                if "first-release-date" in self._json.keys() else None

        except dateutil.parser.ParserError:
            return None

    def __repr__(self):
        s_date = f" {self.first_release_date}" if self.first_release_date is not None else ""
        return f"Recording:  {self.artist_credit_phrase} - {self.title}{s_date} [{self.id}]"

    def __eq__(self, other):
        if isinstance(other, Recording):
            return self.id == other.id
        else:
            return False

    def __lt__(self, other):
        if isinstance(other, Recording):

            if self.first_release_date is not None:
                if other.first_release_date is not None:
                    return self.first_release_date < other.first_release_date
                else:
                    return True
            else:
                return False

    def is_sane(self, artist_query: str, title_query: str, cut_off=70) -> bool:
        artist_sane = any([artist.is_sane(artist_query) for artist in self.artists])

        title_ratio = rapidfuzz.process.extractOne(
            f"{title_query}",
            [self.title]+self.aliases,
            processor=rapidfuzz.utils.default_process
        )[1]

        if not artist_sane:
            _logger.debug(f"{self} is not a sane candidate for artist {artist_query}")
        elif title_ratio < cut_off:
            _logger.debug(f"{self} is not a sane candidate for title {title_query}")
        else:
            return True

    @property
    def url(self) -> str:
        return f"https://musicbrainz.org/recording/{self.id}"

    def __hash__(self):
        return hash(self.id)


class Medium:

    def __init__(self, release: Release, json: dict,
                 search_cache: bool = True,
                 fetch_cache: bool = True):
        self._release: Release = release
        self._json: dict = json
        self._mb_api: MBApi = MBApi(search_cache=search_cache, fetch_cache=fetch_cache)

    @cached_property
    def title(self) -> str:
        return self._json["title"]

    @cached_property
    def position(self) -> str:
        return self._json["position"]

    @cached_property
    def release(self) -> Release:
        return self._release

    @cached_property
    def tracks(self) -> list["Track"]:
        return [Track(self, t) for t in self._json["track-list"]]

    def __repr__(self):
        return f"Medium: {self.release.artist_credit_phrase} - {self.release.title} - {self.title}"


class Track:

    def __init__(self, medium: Medium, json: dict,
                 search_cache: bool = True,
                 fetch_cache: bool = True
                 ):
        self._json: dict = json
        self._id: TrackID = self._json["id"]
        self._medium = medium

        self._mb_api: MBApi = MBApi(search_cache=search_cache, fetch_cache=fetch_cache)

    @cached_property
    def id(self) -> TrackID:
        return self._id

    @cached_property
    def artist_credit_phrase(self) -> str:
        if "artist-credit-phrase" in self._json.keys():
            return self._json["artist-credit-phrase"]
        elif ("artist-credit" in self._json.keys()) and (
                len(self._json["artist-credit"]) == 1
        ):
            return self._json["artist-credit"][0]["artist"]["name"]
        else:
            raise MBApiError("Could not determine artistcredit phrase")

    @cached_property
    def artist_ids(self) -> list[ArtistID]:
        ids = []
        for a in self._json['artist-credit']:
            if isinstance(a, dict):
                ids.append(ArtistID(a['artist']['id']))
        return ids

    @cached_property
    def artists(self) -> list[Artist]:
        return [self._mb_api.get_artist_by_id(x) for x in self.artist_ids]

    @cached_property
    def position(self) -> str:
        return self._json["position"]

    @cached_property
    def number(self) -> str:
        return self._json["number"]

    @property
    def medium(self) -> Medium:
        return self._medium

    @cached_property
    def recording_id(self) -> RecordingID:
        return self._json["recording"]["id"]

    @cached_property
    def recording(self) -> Recording:
        return self._mb_api.get_recording_by_id(self.recording_id)

    @cached_property
    def release(self) -> Release:
        return self.medium.release

    def __repr__(self):
        return f"Track {self.position} of {self.recording.artist_credit_phrase} / {self.release.title} / {self.recording.title}"


class Work:
    def __init__(self, json: dict,
                 search_cache: bool = True,
                 fetch_cache: bool = True):
        self._json = json['work']
        self._id: WorkID = self._json['id']
        self._mb_api: MBApi = MBApi(search_cache=search_cache, fetch_cache=fetch_cache)

    @cached_property
    def id(self) -> WorkID:
        return self._id

    @cached_property
    def title(self) -> str:
        return self._json["title"]

    @cached_property
    def aliases(self) -> list[str]:
        if 'alias-list' in self._json:
            return [x['alias'] for x in self._json['alias-list']]
        return [self.title]

    @cached_property
    def type(self) -> str:
        return self._json["type"]

    @cached_property
    def performance_ids(self) -> dict[str, list[RecordingID]]:
        ids = {"all": [], "no-attr": []}
        if 'recording-relation-list' not in self._json.keys():
            return ids
        for a in self._json['recording-relation-list']:
            if 'recording' in a.keys() and a['direction'] == 'backward':
                if 'attribute-list' in a.keys():
                    for w in a['attribute-list']:
                        if w in ids.keys():
                            ids[w].append(RecordingID(a['recording']['id']))
                        else:
                            ids[w] = [RecordingID(a['recording']['id'])]
                else:
                    ids['no-attr'].append(RecordingID(a['recording']['id']))
                ids['all'].append(RecordingID(a['recording']['id']))
        return ids

    @cached_property
    def performances(self) -> dict[str, list["Recording"]]:
        result = {y: [self._mb_api.get_recording_by_id(x) for x in self.performance_ids[y]] for y in
                  self.performance_ids.keys()}
        return result


    @cached_property
    def disambiguation(self) -> str | None:
        return self._json["disambiguation"] if "disambiguation" in self._json.keys() else None

    def __repr__(self):
        return f"Work:  {self.title}  [{self.id}]"

    def __eq__(self, other):
        if isinstance(other, Work):
            return self.id == other.id
        else:
            return False

    @property
    def url(self) -> str:
        return f"https://musicbrainz.org/work/{self.id}"

    def __hash__(self):
        return hash(self.id)
