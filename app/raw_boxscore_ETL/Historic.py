import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
import glob
import pandas as pd
import json
from multiprocessing import Pool, cpu_count
from loguru import logger
from datetime import date
from app.transforming.generic_df_appenders import df_appender_folder


LOG_FILE = "app/raw_boxscore_ETL/raw_boxscore_ETL_log_log.log"
logger.remove()
logger.add(
    sys.stdout, format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}", level="INFO"
)
logger.add(
    LOG_FILE,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    level="INFO",
    rotation="2 MB",
)


# def extract_boxscore():
#     OUTPUT_DIR = "data/landing/boxscore"

#     ## CONNECT TO
#     db_config = {
#         "dbname": os.getenv("PG_DATABASE"),
#         "user": os.getenv("PG_USER"),
#         "password": os.getenv("PG_PASSWORD"),
#         "host": os.getenv("PG_HOST"),
#         "port": os.getenv("PG_PORT"),
#     }
#     conn = psycopg2.connect(**db_config)
#     cursor = conn.cursor()
#     cursor.execute("SELECT * FROM nhl_raw.stg_csv__gameid_to_get")
#     results = cursor.fetchall()

#     colnames = [desc[0] for desc in cursor.description]

#     df = pd.DataFrame(results, columns=colnames)

#     cursor.close()
#     conn.close()
#     games = df["id"]

#     for game in games:
#         data, _ = make_request(
#             f"https://api-web.nhle.com/v1/gamecenter/{game}/boxscore"
#         )
#         if data:
#             save_json(f"raw_{game}_boxcore", data, OUTPUT_DIR)
#             print(f"Data collected --- {game}")
#         else:
#             print(f"Failed --- {game}")
#     pass

######################################################################################################################################################################


def transform_boxscore_games_worker(input_file):
    try:
        file = os.path.basename(input_file)
        file = os.path.splitext(file)[0]
        file_path = os.path.join("data/csv_data/raw/raw_boxscore_game", file)

        with open(input_file) as f:
            data = json.load(f)

            df = pd.json_normalize(data)

            columns_to_drop = [
                "tvBroadcasts",
                "awayTeam.logo",
                "awayTeam.darkLogo",
                "awayTeam.placeNameWithPreposition.fr",
                "homeTeam.placeNameWithPreposition.fr",
                "homeTeam.commonName.fr",
                "homeTeam.placeName.fr",
                "homeTeam.logo",
                "homeTeam.darkLogo",
                "homeTeam.placeNameWithPreposition.fr",
                "playerByGameStats.awayTeam.forwards",
                "playerByGameStats.awayTeam.forwards",
                "playerByGameStats.awayTeam.defense",
                "playerByGameStats.awayTeam.goalies",
                "playerByGameStats.homeTeam.forwards",
                "playerByGameStats.homeTeam.defense",
                "playerByGameStats.homeTeam.goalies",
                "awayTeam.placeName.fr",
                "venueLocation.fr",
                "gameOutcome.tie",
                "awayTeam.commonName.fr",
                "specialevent_parentid",
                "specialevent_name_default",
                "specialevent_name_fr",
                "perioddescriptor_otperiods",
                "venuelocation_cs",
                "venuelocation_de",
                "venuelocation_fi",
                "venuelocation_sk",
                "venuelocation_sv",
                "specialevent_lightlogourl_default",
                "specialevent_lightlogourl_fr",
                "specialevent_name_sk",
                "specialevent_name_sv",
                "homeTeam.radioLink",
            ]
            df = df.drop(
                columns=[col for col in columns_to_drop if col in df.columns],
                errors="ignore",
            )
            df["filename"] = file
            output_file = file_path + ".csv"
            df.to_csv(output_file, index=False)
            logger.info(f"File saved: {output_file}")
    except Exception as e:
        print(f"Error --- {e}")


def append_boxscore_game():
    today = date.today()
    today = today.strftime("%Y-%m-%d")
    output_file_name = "boxscore_games"
    input_csv_dir = "data/csv_data/staging"
    output_dir = "data/csv_data/processed"
    df_appender_folder(f"{output_file_name}_{today}", input_csv_dir, output_dir)


######################################################################################################################################################################


def transform_boxscore_players_worker(input_file):
    try:
        file = os.path.basename(input_file)
        file = os.path.splitext(file)[0]
        file_path = os.path.join("data/csv_data/raw/raw_boxscore_players", file)

        with open(input_file) as f:
            data = json.load(f)

        record_paths = [
            ["playerByGameStats", "awayTeam", "forwards"],
            ["playerByGameStats", "awayTeam", "defense"],
            ["playerByGameStats", "awayTeam", "goalies"],
            ["playerByGameStats", "homeTeam", "forwards"],
            ["playerByGameStats", "homeTeam", "defense"],
            ["playerByGameStats", "homeTeam", "goalies"],
        ]

        df = pd.concat(
            [pd.json_normalize(data, record_path=path) for path in record_paths],
            ignore_index=True,
        )

        columns_to_drop = [
            "name.cs",
            "name.fi",
            "name.sk",
            "name.sv",
            "name.de",
            "name.es",
            "name.fr",
        ]
        df = df.drop(
            columns=[col for col in columns_to_drop if col in df.columns],
            errors="ignore",
        )
        df["filename"] = file
        output_file = file_path + ".csv"
        df.to_csv(output_file, index=False)
        logger.info(f"File saved: {output_file}")

    except Exception as e:
        logger.error(f"Error processing {input_file}: {e}")


def append_boxscore_players():
    today = date.today()
    today = today.strftime("%Y-%m-%d")
    output_file_name = "boxscore_players"
    input_csv_dir = "data/csv_data/raw/raw_boxscore_players"  # "data/csv_data/staging"
    output_dir = "data/csv_data/processed/first_loads"
    df_appender_folder(f"{output_file_name}", input_csv_dir, output_dir)


if __name__ == "__main__":
    input_files = glob.glob("data/json_data/raw_boxscore/raw_*_details.json")

    with Pool(cpu_count()) as pool:
        pool.map(transform_boxscore_games_worker, input_files)
    pass
