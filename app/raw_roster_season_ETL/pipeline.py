import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import pandas as pd
import shutil
import json
import time
import glob
from loguru import logger
from datetime import date
from app.extraction.generic_get_results import make_request, save_json
from app.transforming.generic_df_appenders import df_appender_folder
from app.loading.data_loader_duckdb import update_table_with_sk


LOG_FILE = "app/raw_roster_season_ETL/raw_rosterseaon_ETL_log_log.log"
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


## raw_roster_season
def extract_team_roster_by_season():
    """ """
    # BUILD URLS
    URL = "https://api-web.nhle.com/v1/roster/{team_id}/{season_id}"
    OUTPUT_DIR = "data/json_data/raw_roster_season/landing"

    parameters_input = "app/api_parameters/seasonid_teamid_game_type.csv"
    df_parameter = pd.read_csv(parameters_input)
    df_parameter = df_parameter[["season_id", "team_id"]].drop_duplicates()

    df_parameter = df_parameter[
        df_parameter["season_id"] == df_parameter["season_id"].max()
    ]

    urls = [
        URL.format(team_id=row.team_id, season_id=row.season_id)
        for row in df_parameter.itertuples()
    ]

    # LOOP THROUGH URLS
    for url in urls:
        try:
            team_id = url.split("/")[-2]
            season_id = url.split("/")[-1]
            file_name = f"raw_roster_{team_id}_{season_id}"
            data, _ = make_request(url)

            save_json(file_name, data, OUTPUT_DIR)
            logger.info(f"Data collected --- {file_name}")
        except Exception as e:
            logger.error(f"Error --- {e}")


def transform_team_roster_by_season():
    PATTERN = "data/json_data/raw_roster_season/landing/raw_roster_*_*.json"
    OUTPUT_DIR = "data/csv_data/raw/raw_roster_season/staging"

    input_files = glob.glob(PATTERN)

    for input_file in input_files:
        try:
            file = os.path.basename(input_file)
            file = os.path.splitext(file)[0]
            file_path = os.path.join(OUTPUT_DIR, file)

            with open(input_file) as f:
                data = json.load(f)

            df_forwards = pd.json_normalize(data=data, record_path=["forwards"])
            df_forwards["role"] = "forward"

            df_defense = pd.json_normalize(data=data, record_path=["defensemen"])
            df_defense["role"] = "defense"

            df_goalies = pd.json_normalize(data=data, record_path=["goalies"])
            df_goalies["role"] = "goalies"

            df = pd.concat([df_forwards, df_defense, df_goalies], ignore_index=True)

            keep_cols = [
                "id",
                "firstName.default",
                "lastName.default",
                "positionCode",
                "sweaterNumber",
                "shootsCatches",
                "birthDate",
                "birthCountry",
                "birthCity.default",
                "heightInCentimeters",
                "heightInInches",
                "weightInKilograms",
                "weightInPounds",
                "headshot",
            ]
            df = df[keep_cols]

            df["filename"] = file
            output_file = file_path + ".csv"
            df.to_csv(output_file, index=False)
            logger.info(f"File saved: {output_file}")
        except Exception as e:
            logger.error(f"Error --- {e}")


def append_team_roster_by_season():
    today = date.today()
    today = today.strftime("%Y-%m-%d")
    OUTPUT_FILE_NAME = "roster_season"
    INPUT_CSV_DIR = "data/csv_data/raw/raw_roster_season/staging"
    OUTPUT_DIR = "data/csv_data/processed"
    try:
        appended_file = df_appender_folder(
            f"{OUTPUT_FILE_NAME}_{today}", INPUT_CSV_DIR, OUTPUT_DIR
        )
        logger.info(f"File {OUTPUT_FILE_NAME} created succesfuly in {OUTPUT_DIR}")
        return appended_file
    except FileNotFoundError as e:
        logger.info(f"File or folder not found --- {e}")


def load_team_roster_by_season(file):
    try:
        update_table_with_sk(file, "raw_roster_season", ["id", "filename"])
        logger.info(f"✅ File {file} has been loaded in nhl_raw.raw_roster_season")
        return file
    except Exception as e:
        logger.error(f"Failed to load {file} --- {e}")


def clear_staging_landing_loading(file):
    JSON_LANDING_DIR = "data/json_data/raw_roster_season/landing"
    JSON_DIR = "data/json_data/raw_roster_season"
    CSV_STAGING_DIR = "data/csv_data/raw/raw_roster_season/staging"
    CSV_DIR = "data/csv_data/raw/raw_roster_season"
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
        extract_team_roster_by_season()
        transform_team_roster_by_season()
        appended_file = append_team_roster_by_season()

        if appended_file:
            loaded_file = load_team_roster_by_season(appended_file)

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
    run()
    pass
