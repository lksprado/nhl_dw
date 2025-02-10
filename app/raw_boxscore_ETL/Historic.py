import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
import glob
import pandas as pd
import json
from multiprocessing import Pool, cpu_count
from loguru import logger


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


if __name__ == "__main__":
    input_files = glob.glob("data/json_data/raw_boxscore/raw_*_details.json")

    with Pool(cpu_count()) as pool:
        pool.map(transform_boxscore_games_worker, input_files)
    pass
