import pymusicbrainz.dataclasses.medium
import pymusicbrainz.dataclasses.track


class Release(MusicBrainzObject):

    def __init__(self,
                 in_obj: ReleaseID | mbdata.models.Release | str) -> None:
        from .object_cache import get_artist
        from .util import parse_partial_date
        from pymusicbrainz.util import area_to_country
        with get_db_session() as session:
            if isinstance(in_obj, mbdata.models.Release):
                rel: mbdata.models.Release = session.merge(in_obj)
            else:
                if isinstance(in_obj, str):
                    in_obj = ReleaseID(in_obj)
                stmt = sa.select(mbdata.models.Release).where(mbdata.models.Release.gid == str(in_obj))
                rel: mbdata.models.Release = session.scalar(stmt)

                if rel is None:
                    raise MBIDNotExistsError(f"No Release with ID '{str(in_obj)}'")

            self.id: ReleaseID = ReleaseID(str(rel.gid))
            self._db_id: int = rel.id
            self.artists = [get_artist(ArtistID(str(a.artist.gid))) for a in rel.artist_credit.artists]
            self.title: str = rel.name
            self._release_group_id: ReleaseGroupID = ReleaseGroupID(str(rel.release_group.gid))
            self.artist_credit_phrase: str = rel.artist_credit.name
            self.disambiguation: str = rel.comment
            self.first_release_date: datetime.date = parse_partial_date(
                rel.first_release.date) if rel.first_release is not None else None
            self.countries: list[str] = [area_to_country(c.country.area) for c in rel.country_dates]

    @cached_property
    def aliases(self) -> list[str]:

        result = [self.title]
        with get_db_session() as session:
            stmt = sa.select(mbdata.models.ReleaseAlias).where(
                mbdata.models.ReleaseAlias.release_id == self._db_id)
            ras: list[mbdata.models.ReleaseAlias] = session.scalars(stmt).all()

            for ra in ras:
                if ra.name not in result:
                    result.append(ra.name)
        return result

    @cached_property
    def url(self) -> str:
        return f"https://musicbrainz.org/release/{self.id}"

    @cached_property
    def is_country_of_artist(self) -> bool:
        return any([a.country in self.countries for a in self.artists])

    @cached_property
    def is_international_release(self) -> bool:
        return any([c in self.countries for c in INT_COUNTRIES])

    @cached_property
    def is_favorite_country(self) -> bool:
        return any([c in self.countries for c in FAVORITE_COUNTRIES])

    @cached_property
    def release_group(self) -> ReleaseGroup:
        from .object_cache import get_release_group
        return get_release_group(self._release_group_id)

    @cached_property
    def mediums(self) -> list["Medium"]:
        from .object_cache import get_medium
        with get_db_session() as session:
            stmt = sa.select(pymusicbrainz.dataclasses.medium.Medium).where(
                pymusicbrainz.dataclasses.medium.Medium.release_id == str(self._db_id))
            ms: list[pymusicbrainz.dataclasses.medium.Medium] = session.scalars(stmt).all()

            return [get_medium(m) for m in ms]

    @cached_property
    def tracks(self) -> list["Track"]:
        result = []
        for m in self.mediums:
            for t in m.tracks:
                if t not in result:
                    result.append(t)
        return result

    @cached_property
    def _recordings_db_items(self) -> list["mbdata.models.Recording"]:
        with get_db_session() as session:
            stmt = (
                sa.select(mbdata.models.Recording)
                .join(pymusicbrainz.dataclasses.track.Track)
                .join(pymusicbrainz.dataclasses.medium.Medium)
                .where(pymusicbrainz.dataclasses.medium.Medium.release.has(id=self._db_id))
            )
            recordings: list[mbdata.models.Recording] = session.scalars(stmt).all()

            return recordings

    @cached_property
    def recordings(self) -> list["Recording"]:
        from .object_cache import get_recording
        return [get_recording(recording) for recording in self._recordings_db_items]

    @cached_property
    def recording_ids(self) -> list["RecordingID"]:
        return [RecordingID(str(recording.gid)) for recording in self._recordings_db_items]

    def is_sane(self, artist_query: str, title_query: str, cut_off=70) -> bool:
        from .util import flatten_title
        artist_ratio = rapidfuzz.fuzz.WRatio(
            flatten_title(artist_name=self.artist_credit_phrase),
            flatten_title(artist_name=artist_query),
            processor=rapidfuzz.utils.default_process,
            score_cutoff=cut_off
        )
        if artist_ratio < cut_off:
            _logger.warning(f"{self} is not a sane candidate for artist {artist_query}")
        title_ratio = rapidfuzz.process.extractOne(
            flatten_title(recording_name=title_query),
            [flatten_title(recording_name=self.title)] + [flatten_title(recording_name=x) for x in self.aliases],
            processor=rapidfuzz.utils.default_process
        )[1]
        if title_ratio < cut_off:
            _logger.warning(f"{self} is not a sane candidate for title {title_query}")
        return artist_ratio > cut_off and title_ratio > cut_off

    def __str__(self):
        s1 = (f" [{self.countries[0]}]" if len(self.countries) == 1 else
              (f" [{self.countries[0]}+{len(self.countries)}]" if len(self.countries) > 1 else "")
              )
        s2 = (
            f" {self.first_release_date}" if self.first_release_date is not None else ""
        )
        return f"'{self.artist_credit_phrase}' - '{self.title}'{s2}{s1} [{self.id}]"

    def __rich__(self):
        s1 = (f" [{self.countries[0]}]" if len(self.countries) == 1 else
              (f" [{self.countries[0]}+{len(self.countries)}]" if len(self.countries) > 1 else "")
              )
        s2 = (
            f" {self.first_release_date}" if self.first_release_date is not None else ""
        )
        return f"'{escape(self.artist_credit_phrase)}' - '{escape(self.title)}'{s2}{s1} \[[link={self.url}]{self.id}[/link]\]"

    def __eq__(self, other):
        if isinstance(other, Release):
            return self.id == other.id
        else:
            return False

    def __contains__(self, item):
        if isinstance(item, Artist):
            return item in self.artists
        if isinstance(item, ReleaseGroup):
            return self.release_group == item
        if isinstance(item, Recording):
            return item in self.recordings
        if isinstance(item, Medium):
            return item.release == item
        if isinstance(item, Track):
            return item.release == item
        if isinstance(item, Work):
            raise NotImplementedError

    def __lt__(self, other):
        if isinstance(other, Release):

            if self.first_release_date is not None:
                if other.first_release_date is not None:
                    if self.first_release_date != other.first_release_date:
                        return self.first_release_date < other.first_release_date
                    elif self.is_country_of_artist != other.is_country_of_artist:
                        return self.is_country_of_artist > other.is_country_of_artist
                    elif self.is_favorite_country != other.is_favorite_country:
                        return self.is_favorite_country > other.is_favorite_country
                    else:
                        #_logger.error("Multiple releases with same date and country:")
                        #_logger.error(self)
                        #_logger.error(other)
                        return True
                else:
                    return True
            else:
                return False

    def __hash__(self):
        return hash(self.id)