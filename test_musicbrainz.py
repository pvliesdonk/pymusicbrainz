import logging

DB_URI: str = "postgresql://musicbrainz:musicbrainz@musicbrainz.int.liesdonk.nl/musicbrainz_db"

if __name__ == "__main__":
    logging.basicConfig(format="%(levelname)-8s:%(asctime)s:%(name)-30s:%(lineno)-4s:%(message)s", level=logging.DEBUG)
    logging.getLogger().setLevel(logging.DEBUG)

    init_database(db_url = DB_URI)
    configure_musicbrainz_api()

    hits = typesense_lookup("DJ Paul Elstak", "Rainbow in the sky")

    a = get_artist(ArtistID("026c4d7c-8dfe-46e8-ab14-cf9304d6863d"))
    b = get_release_group(ReleaseGroupID("94e8bbe7-788d-3000-8e40-57b7591d4fb4"))
    c = get_release(ReleaseID("a6f67b96-5f97-495c-b224-ec93d521f922"))
    d = get_recording(RecordingID("77601dfe-df14-4894-a8b7-c5c68ca25e11"))
    yy = d.performance_of
    xx = d.siblings

    e = get_work(WorkID("3705e2ef-c3d4-3683-9bd7-8574d1749a8d"))
    zz = e.performances

    x = search_artists("DJ Paul Elstak")

    a1 = search_canonical_release("DJ Paul Elstak", "Rainbow in the Sky")

    s2 = search_by_recording_id(RecordingID("77601dfe-df14-4894-a8b7-c5c68ca25e11"))

    y = search_name("DJ Paul Elstak", "Rainbow in the Sky")

    zzzz = find_best_release_group(artist_query="DJ Paul Elstak", title_query="Rainbow in the sky")
    print(zzzz)
    zzzz2 = find_best_release_group(artist_query="DJ Paul Elstak", title_query="Rainbow in the sky", canonical=False)
    print(zzzz2)
    pass
