import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import pandas as pd
import shutil
import json
import glob
import time
from loguru import logger
from datetime import date
from app.extraction.generic_get_results import make_request, save_json
from app.transforming.generic_df_appenders import df_appender_folder
from app.loading.data_loader_duckdb import update_table_with_sk

LOG_FILE = "app/raw_play_by_play/raw_playbyplay_ETL_log_log.log"
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


def extract_play_by_play():
    OUTPUT_DIR = "data/json_data/raw_play_by_play/landing"

    df = pd.read_csv("app/api_parameters/playbyplays_gameids.csv")

    games = df["id"]

    for game_id in games:
        try:
            data, _ = make_request(
                f"https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play"
            )
            save_json(f"raw_{game_id}", data, OUTPUT_DIR)
            logger.info(f"Data collected --- {game_id}")
        except Exception as e:
            logger.error(f"Failed to collect {game_id} --- {e}")


def transform_play_by_play():
    PATTERN = "data/json_data/raw_play_by_play/landing/raw_*.json"
    OUTPUT_DIR = "data/csv_data/raw/raw_play_by_play/staging"

    input_files = glob.glob(PATTERN)
    for input_file in input_files:
        try:
            file = os.path.basename(input_file)
            file = os.path.splitext(file)[0]
            file_path = os.path.join(OUTPUT_DIR, file)

            with open(input_file) as f:
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
                "hometeam.commonName_fr",
                "awayteam.commonName_fr",
                "awayteam.placeName_fr",
                "awayteam.placeName_fr",
            ]
            existing_columns = [col for col in columns_to_drop if col in df.columns]
            if existing_columns:
                df = df.drop(columns=existing_columns)

            df["filename"] = file
            output_file = file_path + ".csv"
            df.to_csv(output_file, index=False)
            logger.info(f"File saved: {output_file}")
        except Exception as e:
            logger.error(f"Error --- {e}")


def append_play_by_play():
    today = date.today()
    today = today.strftime("%Y-%m-%d")
    OUTPUT_FILE_NAME = "raw_play_by_play"
    INPUT_CSV_DIR = "data/csv_data/raw/raw_play_by_play/staging"
    OUTPUT_DIR = "data/csv_data/processed"
    try:
        appended_file = df_appender_folder(
            f"{OUTPUT_FILE_NAME}_{today}", INPUT_CSV_DIR, OUTPUT_DIR
        )
        logger.info(f"File {OUTPUT_FILE_NAME} created succesfuly in {OUTPUT_DIR}")
        return appended_file
    except FileNotFoundError as e:
        logger.info(f"File or folder not found --- {e}")


def load_play_by_play(file):
    try:
        update_table_with_sk(file, "raw_play_by_play", ["eventid", "filename"])
        logger.info(f"✅ File {file} has been loaded in nhl_raw.raw_play_by_play")
        return file
    except Exception as e:
        logger.error(f"Failed to load {file} --- {e}")


def clear_staging_landing_loading(file):
    JSON_LANDING_DIR = "data/json_data/raw_play_by_play/landing"
    JSON_DIR = "data/json_data/raw_play_by_play"
    CSV_STAGING_DIR = "data/csv_data/raw/raw_play_by_play/staging"
    CSV_DIR = "data/csv_data/raw/raw_play_by_play"
    LOAD_DIR = "data/csv_data/processed"
    STORED_LOADS = "data/csv_data/processed/flow_loads"

    ## MOVE JSON FILES
    try:
        for filename in os.listdir(JSON_LANDING_DIR):
            source_path = os.path.join(JSON_LANDING_DIR, filename)
            destination_path = os.path.join(JSON_DIR, filename)
            shutil.move(source_path, destination_path)
        logger.info(f"Json files in {JSON_LANDING_DIR} moved to {JSON_DIR}")
    except Exception as e:
        logger.warning(f"Failed to move json files --- {e}")

    ## MOVE CSV FILES
    try:
        for filename in os.listdir(CSV_STAGING_DIR):
            source_path = os.path.join(CSV_STAGING_DIR, filename)
            destination_path = os.path.join(CSV_DIR, filename)
            shutil.move(source_path, destination_path)
        logger.info(f"Csv files in {CSV_STAGING_DIR} moved to {CSV_DIR}")
    except Exception as e:
        logger.warning(f"Failed to move csv files {e}")

    ## MOVE LOAD FILE
    try:
        file_only = os.path.basename(file)
        source_path = os.path.join(LOAD_DIR, file_only)
        destination_path = os.path.join(STORED_LOADS, file_only)
        shutil.move(source_path, destination_path)
    except Exception as e:
        logger.warning(f"Failed to move loaded csv file -- {e}")


def run():
    start_time = time.time()

    try:
        extract_play_by_play()
        transform_play_by_play()
        appended_file = append_play_by_play()

        if appended_file:
            loaded_file = load_play_by_play(appended_file)

            if loaded_file:
                clear_staging_landing_loading(loaded_file)

        end_time = time.time()
        elapsed_time = end_time - start_time
        minutes = int(elapsed_time // 60)
        seconds = elapsed_time % 60
        logger.info(
            f"✅✅✅ Pipeline completed successfully in {minutes}min {seconds:.0f}s."
        )

    except Exception as e:
        logger.error(f"❌❌❌ Pipeline failed --- {e}")


if __name__ == "__main__":
    # extract_play_by_play()
    # transform_play_by_play()
    # append_play_by_play()
    # load_play_by_play()
    run()
    pass
