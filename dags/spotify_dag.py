from datetime import datetime

import spotipy
from airflow.decorators import dag, task
from airflow.operators.postgres_operator import PostgresOperator
from spotipy.oauth2 import SpotifyClientCredentials
import os
import csv


csv_path = "/opt/sources/songs.csv"


@task
def get_spotify_data():
    sp = spotipy.Spotify(client_credentials_manager=SpotifyClientCredentials())

    playlist_URI = os.environ["SPOTIPY_DISCOVER_WEEKLY_URI"]
    playlist_data = sp.playlist_tracks(playlist_URI)
    with open(csv_path, "w") as f:
        write = csv.writer(f, delimiter=",")
        write.writerow(["artist", "track_name", "spotify_added"])
        for item in playlist_data["items"]:
            spotify_added = item["added_at"]
            track_name = item["track"]["name"]
            # track_uri = item["track"]["uri"]
            artist = item["track"]["artists"][0]["name"]
            # artist_uri = item["track"]["artists"][0]["uri"]
            write.writerow([artist, track_name, spotify_added])


@dag(schedule="@weekly", start_date=datetime(2021, 1, 1), catchup=False)
def spotify_pipeline():
    create_source_table = PostgresOperator(
        task_id="create_source_table",
        sql="""
            CREATE TABLE IF NOT EXISTS spotify_source (
            id SERIAL PRIMARY KEY,
            artist VARCHAR NOT NULL,
            track_name VARCHAR NOT NULL,
            spotify_added DATE NOT NULL,
            load_ts DATE NOT NULL DEFAULT now(),
            track_sk bigint GENERATED ALWAYS AS (hash_record_extended((artist, track_name), 0)) STORED
            );
          """,
    )

    create_weekly_table = PostgresOperator(
        task_id="create_weekly_table",
        sql="""
            CREATE TABLE IF NOT EXISTS spotify_weekly (
            id SERIAL PRIMARY KEY,
            artist VARCHAR NOT NULL,
            track_name VARCHAR NOT NULL,
            spotify_added DATE NOT NULL,
            track_sk bigint
            );
          """,
    )

    populate_source_table = PostgresOperator(
        task_id="populate_source_table",
        sql=f"""
            COPY spotify_source(artist, track_name, spotify_added)
            FROM '{csv_path}'
            DELIMITER ','
            CSV HEADER;
            """,
    )

    populate_weekly_table = PostgresOperator(
        task_id="populate_weekly_table",
        sql="""
            TRUNCATE TABLE spotify_weekly;
            WITH
            incremental AS
                (SELECT 
                    ROW_NUMBER()
                        OVER(PARTITION BY track_sk
                             ORDER BY load_ts DESC) AS rn
                    , *
                FROM spotify_source)
            INSERT INTO
                spotify_weekly(artist, track_name, spotify_added, track_sk)
            SELECT 
                artist
                , track_name
                , spotify_added
                , track_sk
            FROM incremental
            WHERE rn = 1;
          """,
    )

    (
        get_spotify_data()
        >> [create_source_table, create_weekly_table]
        >> populate_source_table
        >> populate_weekly_table
    )


spotify_pipeline()
