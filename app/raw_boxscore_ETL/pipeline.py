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
from app.loading.data_loader_duckdb import update_table_with_sk, update_table_with_pk


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


def extract_boxscore():
    OUTPUT_DIR = "data/json_data/raw_boxscore/landing"

    games = pd.read_csv("app/api_parameters/boxscore_gameids.csv")

    for game in games["id"]:
        data, _ = make_request(
            f"https://api-web.nhle.com/v1/gamecenter/{game}/boxscore"
        )
        save_json(f"raw_{game}_details", data, OUTPUT_DIR)
        logger.info(f"Data Collect --- {game}")
    logger.info("Data collection complete")


######################################################################################################################################################################


def transform_boxscore_games():
    PATTERN = "data/json_data/raw_boxscore/landing/raw_*_details.json"
    OUTPUT_DIR = "data/csv_data/raw/raw_boxscore_game/staging"

    input_files = glob.glob(PATTERN)

    for input_file in input_files:
        try:
            file = os.path.basename(input_file)
            file = os.path.splitext(file)[0]
            file_path = os.path.join(OUTPUT_DIR, file)

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
                    "specialEvent.parentId",
                    "specialEvent.name.default",
                    "specialEvent.name.fr",
                    "periodDescriptor.otPeriods",
                    "venueLocation.cs",
                    "venueLocation.de",
                    "venueLocation.fi",
                    "venueLocation.sk",
                    "venueLocation.sv",
                    "specialEvent.lightLogoUrl.default",
                    "specialEvent.lightLogoUrl.fr",
                    "specialEvent.name.sk",
                    "specialEvent.name.sv",
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
            logger.error(f"Error --- {e}")


def append_boxscore_game():
    today = date.today()
    today = today.strftime("%Y-%m-%d")
    OUTPUT_FILE_NAME = "boxscore_games"
    INPUT_CSV_DIR = "data/csv_data/raw/raw_boxscore_game/staging"
    OUTPUT_DIR = "data/csv_data/processed"
    try:
        appended_file = df_appender_folder(
            f"{OUTPUT_FILE_NAME}_{today}", INPUT_CSV_DIR, OUTPUT_DIR
        )
        logger.info(f"File {OUTPUT_FILE_NAME} created succesfuly in {OUTPUT_DIR}")
        return appended_file

    except FileNotFoundError as e:
        logger.info(f"File or folder not found --- {e}")


def load_boxscore_game(file):
    try:
        update_table_with_pk(
            file,
            "raw_boxscore_games",
            "id",
        )
        logger.info(f"✅ File {file} has been loaded in nhl_raw.raw_boxscore_games")
        return file
    except Exception as e:
        logger.error(f"Failed to load {file} --- {e}")


######################################################################################################################################################################


def transform_boxscore_players():
    PATTERN = "data/json_data/raw_boxscore/landing/raw_*_details.json"
    OUTPUT_DIR = "data/csv_data/raw/raw_boxscore_players/staging"
    input_files = glob.glob(PATTERN)

    for input_file in input_files:
        try:
            file = os.path.basename(input_file)
            file = os.path.splitext(file)[0]
            file_path = os.path.join(OUTPUT_DIR, file)

            with open(input_file) as f:
                data = json.load(f)

            # Use record_path as a list of paths
            record_paths = [
                ["playerByGameStats", "awayTeam", "forwards"],
                ["playerByGameStats", "awayTeam", "defense"],
                ["playerByGameStats", "awayTeam", "goalies"],
                ["playerByGameStats", "homeTeam", "forwards"],
                ["playerByGameStats", "homeTeam", "defense"],
                ["playerByGameStats", "homeTeam", "goalies"],
            ]

            # Combine results from all paths
            df = pd.concat(
                [pd.json_normalize(data, record_path=path) for path in record_paths],
                ignore_index=True,
            )

            # Drop unnecessary columns
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
            logger.error(f"Error --- {e}")


def append_boxscore_players():
    today = date.today()
    today = today.strftime("%Y-%m-%d")
    OUTPUT_FILE_NAME = "boxscore_players"
    INPUT_CSV_DIR = "data/csv_data/raw/raw_boxscore_players/staging"
    OUTPUT_DIR = "data/csv_data/processed"
    try:
        appended_file = df_appender_folder(
            f"{OUTPUT_FILE_NAME}_{today}", INPUT_CSV_DIR, OUTPUT_DIR
        )
        logger.info(f"File {OUTPUT_FILE_NAME} created succesfuly in {OUTPUT_DIR}")
        return appended_file

    except FileNotFoundError as e:
        logger.info(f"File or folder not found --- {e}")


def load_boxscore_players(file):
    try:
        update_table_with_sk(
            file,
            "raw_boxscore_players",
            ["playerid", "filename", "points"],
        )
        logger.info(f"✅ File {file} has been loaded in nhl_raw.raw_boxscore_players")
        return file
    except Exception as e:
        logger.error(f"Failed to load {file} --- {e}")


######################################################################################################################################################################


def clear_staging_landing_loading(file):
    JSON_LANDING_DIR = "data/json_data/raw_boxscore/landing"
    JSON_DIR = "data/json_data/raw_boxscore"
    CSV_STAGING_DIR_GAMES = "data/csv_data/raw/raw_boxscore_game/staging"
    CSV_DIR_GAMES = "data/csv_data/raw/raw_boxscore_game"
    CSV_STAGING_DIR_PLAYERS = "data/csv_data/raw/raw_boxscore_players/staging"
    CSV_DIR_PLAYERS = "data/csv_data/raw/raw_boxscore_players"
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
        for filename in os.listdir(CSV_STAGING_DIR_GAMES):
            source_path = os.path.join(CSV_STAGING_DIR_GAMES, filename)
            destination_path = os.path.join(CSV_DIR_GAMES, filename)
            shutil.move(source_path, destination_path)
        logger.info(f"Csv files in {CSV_STAGING_DIR_GAMES} moved to {CSV_DIR_GAMES}")
    except FileNotFoundError:
        logger.warning("Failed to move csv files")

    ## MOVE CSV FILES
    try:
        for filename in os.listdir(CSV_STAGING_DIR_PLAYERS):
            source_path = os.path.join(CSV_STAGING_DIR_PLAYERS, filename)
            destination_path = os.path.join(CSV_DIR_PLAYERS, filename)
            shutil.move(source_path, destination_path)
        logger.info(
            f"Csv files in {CSV_STAGING_DIR_PLAYERS} moved to {CSV_DIR_PLAYERS}"
        )
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
    start_time = time.time()

    try:
        extract_boxscore()

        transform_boxscore_games()

        appended_file_games = append_boxscore_game()

        if appended_file_games:
            loaded_file_games = load_boxscore_game(appended_file_games)

        transform_boxscore_players()

        appended_file_players = append_boxscore_players()

        if appended_file_players:
            loaded_file_players = load_boxscore_players(appended_file_players)

        if loaded_file_games:
            clear_staging_landing_loading(loaded_file_games)

        if loaded_file_players:
            clear_staging_landing_loading(loaded_file_players)

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
    # extract_boxscore()
    # transform_boxscore_games()
    # append_boxscore_game()
    # append_boxscore_players()s
    # create_and_load_table_with_pk('data/csv_data/processed/boxscore_games_2025-02-10.csv', 'nhl_raw.raw_boxscore_games','id')
    # create_and_load_table_with_sk('data/csv_data/processed/boxscore_players_2025-02-10.csv', 'nhl_raw.raw_boxscore_players',['playerid','filename','points'])
    run()
    pass
