import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
import shutil
import glob
import pandas as pd
import json
import time
from loguru import logger
from datetime import date
from app.extraction.generic_get_results import make_request, save_json
from app.transforming.generic_df_appenders import df_appender_folder
from app.loading.data_loader_duckdb import update_table_with_sk

LOG_FILE = "app/raw_all_club_stats_ETL/raw_all_club_stats_log.log"
logger.remove()
logger.add(
    sys.stdout, format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}", level="INFO"
)
logger.add(
    LOG_FILE,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    level="INFO",
    rotation="10 MB",
)


def fetch_and_save_club_stats(team_id, season_id, game_type, url, output_dir):
    """Abstract function for multi-threading use"""
    data, _ = make_request(url)
    save_json(f"raw_stats_club_{team_id}_{season_id}_{game_type}", data, output_dir)


def extract_club_stats():
    """
    #### Daily Update
    Makes the API call based on parameters and saves json file
    """

    URL = "https://api-web.nhle.com/v1/club-stats/{team_id}/{season_id}/{game_type}"
    OUTPUT_DIR = "data/json_data/raw_club_stats/landing"

    parameters_input = "app/api_parameters/seasonid_teamid_game_type.csv"
    df_parameter = pd.read_csv(parameters_input)
    df_parameter = df_parameter.sort_values(
        by=["season_id", "team_id", "game_type"], ascending=False
    )

    max_season_id = df_parameter["season_id"].max()
    df_filtered = df_parameter[df_parameter["season_id"] == max_season_id]

    urls = [
        URL.format(
            team_id=row.team_id, season_id=row.season_id, game_type=row.game_type
        )
        for row in df_filtered.itertuples()
    ]

    for url in urls:
        try:
            team_id = url.split("/")[-3]
            season_id = url.split("/")[-2]
            game_type = url.split("/")[-1]
            file_name = f"raw_stats_club_{team_id}_{season_id}_{game_type}"
            data, _ = make_request(url)
            save_json(file_name, data, OUTPUT_DIR)
            logger.info(f"Data Collect --- {file_name}")
        except Exception as e:
            logger.error(f"Failed to collect {url} --- {e}")


def transform_club_stats():
    """Transform all json to csv files"""
    PATTERN = "data/json_data/raw_club_stats/landing/raw_stats_club_*_*_*.json"
    OUTPUT_DIR = "data/csv_data/raw/raw_club_stats/staging"
    input_files = glob.glob(PATTERN)
    for input_file in input_files:
        try:
            file = os.path.basename(input_file)
            file = os.path.splitext(file)[0]
            file_path = os.path.join(OUTPUT_DIR, file)
            with open(input_file) as f:
                data = json.load(f)

            df_skaters = pd.json_normalize(
                data,
                meta=["season", "gameType"],
                record_path=["skaters"],
                errors="ignore",
            )
            df_skaters["filename"] = file

            df_goalies = pd.json_normalize(
                data,
                meta=["season", "gameType"],
                record_path=["goalies"],
                errors="ignore",
            )
            df_goalies["filename"] = file

            drop = [
                "lastName.cs",
                "firstName.cs",
                "firstName.sk",
                "lastName.sk",
                "firstName.de",
                "firstName.es",
                "firstName.fi",
                "firstName.sv",
                "lastName.fi",
                "lastName.sv",
                "lastName.cs",
                "lastName.sk",
                "firstName.fr",
                "lastName.de",
                "lastName.fi",
                "lastName.es",
                "lastName.sv",
                "lastName.fr",
            ]

            df = pd.concat([df_skaters, df_goalies], ignore_index=True)
            df = df.drop(columns=drop, errors="ignore")
            output_file = file_path + ".csv"
            df.to_csv(output_file, index=False)
        except Exception as e:
            logger.error(f"Error -- {e}")

    number_of_files = len(os.listdir(OUTPUT_DIR))
    logger.info(f"{number_of_files} files saved: {file_path}")


def append_club_stats():
    """Concatenates all csv files to a single one for loading purposes"""
    today = date.today()
    today = today.strftime("%Y-%m-%d")
    OUTPUT_FILE_NAME = "all_club_stats"
    INPUT_CSV_DIR = "data/csv_data/raw/raw_club_stats/staging"
    OUTPUT_CSV_DIR = "data/csv_data/processed"

    try:
        appended_file = df_appender_folder(
            f"{OUTPUT_FILE_NAME}_{today}", INPUT_CSV_DIR, OUTPUT_CSV_DIR
        )
        logger.info(f"File {OUTPUT_FILE_NAME} created succesfuly in {OUTPUT_CSV_DIR}")
        return appended_file

    except FileNotFoundError as e:
        logger.info(f"File or folder not found --- {e}")


def load_club_stats(file):
    """Loads csv file through upserting"""
    try:
        update_table_with_sk(
            file,
            "raw_all_club_stats",
            ["playerid", "gamesplayed", "positioncode"],
        )
        logger.info(f"✅ File {file} has been loaded in nhl_raw.raw_all_club_stats")
        return file
    except Exception as e:
        logger.error(f"Failed to load {file} --- {e}")


def clear_staging_landing_loading(file):
    """Move files for storage"""
    JSON_LANDING_DIR = "data/json_data/raw_club_stats/landing"
    JSON_DIR = "data/json_data/raw_club_stats"
    CSV_STAGING_DIR = "data/csv_data/raw/raw_club_stats/staging"
    CSV_DIR = "data/csv_data/raw/raw_club_stats"
    LOAD_DIR = "data/csv_data/processed"
    STORED_LOADS = "data/csv_data/processed/flow_loads"

    ## MOVE JSON FILES
    try:
        for filename in os.listdir(JSON_LANDING_DIR):
            source_path = os.path.join(JSON_LANDING_DIR, filename)
            destination_path = os.path.join(JSON_DIR, filename)
            shutil.move(source_path, destination_path)
        logger.info(f"Json files in {JSON_LANDING_DIR} moved to {JSON_DIR}")
    except FileNotFoundError as e:
        logger.warning(f"Failed to move json files --- {e}")

    ## MOVE CSV FILES
    try:
        for filename in os.listdir(CSV_STAGING_DIR):
            source_path = os.path.join(CSV_STAGING_DIR, filename)
            destination_path = os.path.join(CSV_DIR, filename)
            shutil.move(source_path, destination_path)
        logger.info(f"Csv files in {CSV_STAGING_DIR} moved to {CSV_DIR}")
    except FileNotFoundError:
        logger.warning("Failed to move csv files")

    ## MOVE LOAD FILE
    try:
        file_only = os.path.basename(file)
        source_path = os.path.join(LOAD_DIR, file_only)
        destination_path = os.path.join(STORED_LOADS, file_only)
        shutil.move(source_path, destination_path)
    except FileNotFoundError as e:
        logger.warning(f"Failed to move loaded csv file -- {e}")


def run():
    try:
        start_time = time.time()

        extract_club_stats()
        transform_club_stats()
        appended_file = append_club_stats()
        loaded_file = load_club_stats(appended_file)
        clear_staging_landing_loading(loaded_file)

        end_time = time.time()
        elapsed_time = end_time - start_time
        minutes = int(elapsed_time // 60)
        seconds = elapsed_time % 60
        logger.info(
            f"✅✅✅ Pipeline completed succesfuly in {minutes}min. {seconds:.0f}s."
        )
    except Exception as e:
        logger.error(f"❌❌❌ Pipeline failed --- {e}")


if __name__ == "__main__":
    run()
    pass
