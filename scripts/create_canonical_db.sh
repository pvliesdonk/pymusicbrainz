#!/bin/bash
# get canonical dataset from https://data.metabrainz.org/pub/musicbrainz/canonical_data/
# e.g. "musicbrainz-canonical-dump-20240903-080003.tar.zst"
# then extract canonical_recording_redirect.csv and canonical_release_redirect.csv


sqlite3 canonical_redirect.db <<EOF
.mode csv
create table canonical_recording_redirect(recording_mbid PRIMARY KEY,canonical_recording_mbid,canonical_release_mbid);
create table canonical_release_redirect (release_mbid PRIMARY KEY,canonical_release_mbid,release_group_mbid);
create table metadata(key PRIMARY KEY, attribute);


.import canonical_recording_redirect.csv canonical_recording_redirect
.import canonical_release_redirect.csv canonical_release_redirect

insert into metadata (key,attribute) values ('date', date());

vacuum;
.exit
EOF