class Recording(MusicBrainzObject):

    def __init__(self,
                 in_obj: RecordingID | mbdata.models.Recording | str) -> None:
        from .object_cache import get_artist
        from .util import parse_partial_date
        with get_db_session() as session:
            if isinstance(in_obj, mbdata.models.Recording):
                rec: mbdata.models.Recording = session.merge(in_obj)
            else:
                if isinstance(in_obj, str):
                    in_obj = RecordingID(in_obj)
                stmt = sa.select(mbdata.models.Recording).where(mbdata.models.Recording.gid == str(in_obj))
                rec: mbdata.models.Recording = session.scalar(stmt)
                if rec is None:
                    raise MBIDNotExistsError(f"No recording with id '{in_obj}'")

            self.id: RecordingID = RecordingID(str(rec.gid))
            self._db_id: int = rec.id
            self.artists = [get_artist(ArtistID(str(a.artist.gid))) for a in rec.artist_credit.artists]
            self.title: str = rec.name
            self.artist_credit_phrase: str = rec.artist_credit.name
            self.disambiguation: str = rec.comment
            self.first_release_date: datetime.date = parse_partial_date(
                rec.first_release.date) if rec.first_release is not None else None

    @cached_property
    def aliases(self) -> list[str]:
        result = [self.title]
        with get_db_session() as session:
            stmt = sa.select(mbdata.models.RecordingAlias).where(
                mbdata.models.RecordingAlias.recording_id == self._db_id)
            ras: list[mbdata.models.RecordingAlias] = session.scalars(stmt).all()

            for ra in ras:
                if ra.name not in result:
                    result.append(ra.name)
        return result

    @cached_property
    def performance_type(self) -> list[PerformanceWorkAttributes]:
        p = self.performance_of
        return self.performance_type

    @cached_property
    def performance_of(self) -> list["Work"]:
        from .object_cache import get_work
        with get_db_session() as session:
            stmt = sa.select(mbdata.models.LinkRecordingWork). \
                where(mbdata.models.LinkRecordingWork.entity0_id == str(self._db_id))
            res: list[mbdata.models.LinkRecordingWork] = session.scalars(stmt).all()
            if res is None or len(res) == 0:
                self.performance_type = []
                return []
            else:
                ws = [get_work(r.work) for r in res]

            types = []
            for r in res:
                stmt = sa.select(mbdata.models.LinkAttribute). \
                    where(mbdata.models.LinkAttribute.link == r.link)
                res2: list[mbdata.models.LinkAttribute] = session.scalars(stmt).all()

                [types.append(PerformanceWorkAttributes(att.attribute_type.name)) for att in res2 if PerformanceWorkAttributes(att.attribute_type.name) not in types]

            self.performance_type = types

        return ws

    @cached_property
    def is_acapella(self) -> bool:
        return PerformanceWorkAttributes.ACAPELLA in self.performance_type

    @cached_property
    def is_live(self) -> bool:
        return PerformanceWorkAttributes.LIVE in self.performance_type

    @cached_property
    def is_medley(self) -> bool:
        return PerformanceWorkAttributes.MEDLEY in self.performance_type

    @cached_property
    def is_partial(self) -> bool:
        return PerformanceWorkAttributes.PARTIAL in self.performance_type

    @cached_property
    def is_instrumental(self) -> bool:
        return PerformanceWorkAttributes.INSTRUMENTAL in self.performance_type

    @cached_property
    def is_cover(self) -> bool:
        return PerformanceWorkAttributes.COVER in self.performance_type

    @cached_property
    def is_karaoke(self) -> bool:
        return PerformanceWorkAttributes.KARAOKE in self.performance_type

    @cached_property
    def is_normal_performance(self) -> bool:
        return len(self.performance_type) == 0

    @cached_property
    def siblings(self) -> list["Recording"]:
        result = []
        _logger.debug(f"Computing siblings of {self}")
        works = self.performance_of
        for work in works:
            if len(self.performance_type) == 0:
                for r in work.performance_by_type([PerformanceWorkAttributes.NONE]):
                    if r not in result and r.artists == self.artists:
                        result.append(r)
            else:
                _logger.debug(
                    f"Recording of types {'/'.join(self.performance_type)}; returning matching siblings of {self.artist_credit_phrase} - {self.title}")

                result = [rec for rec in work.performance_by_type(self.performance_type) if rec.artists == self.artists]
        _logger.debug(f"Identified {len(result)} siblings")
        return result



    # @cached_property
    # def streams(self) -> list[str]:
    #     result = []
    #     with get_db_session() as session:
    #
    #         base_stmt = (
    #             sa.select(mbdata.models.URL, mbdata.models.Link, mbdata.models.LinkAttribute)
    #             .select_from(
    #                 sa.join(
    #                     sa.join(mbdata.models.URL, mbdata.models.LinkRecordingURL).join(mbdata.models.Recording),
    #                     sa.join(mbdata.models.Link, mbdata.models.LinkAttribute),
    #                     isouter=True
    #                 ))
    #         )
    #         stmt = base_stmt.where(mbdata.models.LinkRecordingURL.recording_id == str(self._db_id))
    #
    #         res: sa.ChunkedIteratorResult = session.execute(stmt)
    #
    #         if res.raw.rowcount == 0:
    #             _logger.debug(f"Also looking for streams of siblings")
    #
    #             siblings = [str(s.id) for s in self.siblings]
    #
    #             stmt = base_stmt.where(mbdata.models.Recording.gid.in_(siblings))
    #             res: list[mbdata.models.URL, mbdata.models.Link, mbdata.models.LinkAttribute] = session.execute(stmt)
    #
    #         for (url, link, la) in res:
    #             if la is not None:
    #                 if la.attribute_type_id == 582:  # video
    #                     continue
    #             if url.url not in result:
    #                 result.append(url.url)
    #
    #     return result
    #
    # @cached_property
    # def spotify_id(self) -> str | None:
    #     spotify_id_regex = r'open\.spotify\.com/\w+/([0-9A-Za-z]+)'
    #     for url in self.streams:
    #         match = re.search(spotify_id_regex, url)
    #         if match:
    #             id_ = match.group(1)
    #             if id_:
    #                 return id_
    #     return None

    def __str__(self):
        s_date = f" {self.first_release_date}" if self.first_release_date is not None else ""
        return f"'{self.artist_credit_phrase}' - '{self.title}'{s_date} [{self.id}] " + (
            "/".join(self.performance_type) if len(self.performance_type) > 0 else "")


    def __rich__(self):
        s_date = f" {self.first_release_date}" if self.first_release_date is not None else ""
        return f"'{escape(self.artist_credit_phrase)}' - '{escape(self.title)}'{s_date} \[[link={self.url}]{self.id}[/link]\] " + (
            "/".join(self.performance_type) if len(self.performance_type) > 0 else "")

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

    def __contains__(self, item):
        if isinstance(item, Artist):
            return item in self.artists
        if isinstance(item, ReleaseGroup):
            return self in item.recordings
        if isinstance(item, Release):
            return self in item.recordings
        if isinstance(item, Medium):
            return any([self == t.recording for t in item.tracks])
        if isinstance(item, Track):
            return item.recording == self
        if isinstance(item, Work):
            return self in item.performances['all']

    def is_sane(self, artist_query: str, title_query: str, cut_off=70) -> bool:
        from .util import flatten_title
        artist_sane = any([artist.is_sane(artist_query) for artist in self.artists])

        title_ratio = rapidfuzz.process.extractOne(
            flatten_title(recording_name=title_query),
            [flatten_title(recording_name=self.title)] + [flatten_title(recording_name=a) for a in
                                                          self.aliases],
            processor=rapidfuzz.utils.default_process
        )[1]

        if not artist_sane:
            _logger.warning(f"{self} is not a sane candidate for artist {artist_query}")
        elif title_ratio < cut_off:
            _logger.warning(f"{self} is not a sane candidate for title {title_query}")
        else:
            return True

    @cached_property
    def url(self) -> str:
        return f"https://musicbrainz.org/recording/{self.id}"

    def __hash__(self):
        return hash(self.id)