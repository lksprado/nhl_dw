import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))


import pandas as pd
import json
import psycopg2
from app.extraction.generic_get_results import make_request, save_json
from app.transforming.generic_df_appenders import df_appender_folder
from app.loading.data_loader_duckdb import create_and_upsert_temp_table
from datetime import date


from dotenv import load_dotenv

load_dotenv()


def extract_play_by_play():
    OUTPUT_DIR = "data/json_data/raw_play_by_play/landing"

    ## CONNECT TO
    db_config = {
        "dbname": os.getenv("PG_DATABASE"),
        "user": os.getenv("PG_USER"),
        "password": os.getenv("PG_PASSWORD"),
        "host": os.getenv("PG_HOST"),
        "port": os.getenv("PG_PORT"),
    }

    QUERY = """
            with
            agenda AS (
                SELECT
                *
                FROM nhl_raw.raw_game_info
                WHERE
                "gamestateid" IN ('1','7')
            AND "gameschedulestateid" = '1'
            AND "gametype" IN ('2','3')
            AND "period" IS NOT NULL
                ),
            got_data AS (
                SELECT SUBSTRING("filename" FROM 5 FOR 10) as id FROM nhl_raw.raw_play_by_play
                )
            SELECT a.id FROM agenda a
            LEFT JOIN got_data gd
            ON a.id = gd.id
            WHERE gd.id IS NULL
            AND to_date(a."gamedate",'YYYY-MM-DD') < current_date
            ORDER BY id desc
            """
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    cursor.execute(QUERY)
    results = cursor.fetchall()

    colnames = [desc[0] for desc in cursor.description]

    df = pd.DataFrame(results, columns=colnames)

    cursor.close()
    conn.close()
    games = df["id"]

    for game_id in games:
        data, _ = make_request(
            f"https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play"
        )
        if data:
            save_json(f"raw_{game_id}", data, OUTPUT_DIR)
            print(f"Data collected --- {game_id}")
        else:
            print(f"Failed --- {game_id}")


def transform_play_by_play():
    INPUT_DIR = "data/json_data/raw_play_by_play/landing"
    OUTPUT_DIR = "data/csv_data/raw/raw_play_by_play/staging/"

    json_dir = os.listdir(INPUT_DIR)
    for file in json_dir:
        try:
            j = os.path.join(INPUT_DIR, file)
            csv_filename = os.path.splitext(os.path.basename(j))[0]

            with open(j) as f:
                data = json.load(f)
                df = pd.DataFrame()
                if "plays" in data:
                    df = pd.json_normalize(data, record_path=["plays"])
                if "awayTeam" in data:
                    away_team = pd.json_normalize(data["awayTeam"])
                    for col in away_team.columns:
                        df[f"awayTeam.{col}"] = away_team.loc[0, col]
                if "homeTeam" in data:
                    home_team = pd.json_normalize(data["homeTeam"])
                    for col in home_team.columns:
                        df[f"homeTeam.{col}"] = home_team.loc[0, col]
                if not df.empty:
                    df["filename"] = file

            columns_to_drop = [
                "details.highlightClipSharingUrlFr",
                "details.highlightClip",
                "details.highlightClipFr",
                "details.discreteClip",
                "details.discreteClipFr",
                "awayTeam.logo",
                "awayTeam.darkLogo",
                "awayTeam.placeNameWithPreposition.default",
                "awayTeam.placeNameWithPreposition.fr",
                "homeTeam.logo",
                "homeTeam.darkLogo",
                "homeTeam.placeNameWithPreposition.default",
                "homeTeam.placeNameWithPreposition.fr",
                "hometeam_commonname_fr",
                "awayteam_commonname_fr",
                "awayteam_placename_fr",
                "awayteam_placename_fr",
            ]
            existing_columns = [col for col in columns_to_drop if col in df.columns]
            if existing_columns:
                df = df.drop(columns=existing_columns)

            df["filename"] = csv_filename
            print(f"Saved --- {csv_filename}")
            df.to_csv(f"{OUTPUT_DIR}/{csv_filename}.csv", index=False)
        except Exception as e:
            print(f"Failed to parse {j} --- {e}")


def append_play_by_play():
    today = date.today()
    today = today.strftime("%Y-%m-%d")
    output_file_name = "raw_play_by_play"
    input_csv_dir = "data/csv_data/raw/raw_play_by_play/staging"
    output_dir = "data/csv_data/processed"
    df_appender_folder(f"{output_file_name}_{today}", input_csv_dir, output_dir)


def load_play_by_play():
    # create_and_load_table('data/csv_data/processed/first_loads/play_by_play.csv', 'raw_play_by_play','eventid')
    create_and_upsert_temp_table(
        "data/csv_data/processed/raw_play_by_play_2025-02-07.csv",
        "raw_play_by_play",
        "eventid",
    )


if __name__ == "__main__":
    # extract_play_by_play()
    # transform_play_by_play()
    # append_play_by_play()
    load_play_by_play()
    pass
