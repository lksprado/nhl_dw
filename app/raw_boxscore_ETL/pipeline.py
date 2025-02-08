import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from datetime import date
import json
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from app.extraction.generic_get_results import make_request, save_json
from app.transforming.generic_df_appenders import df_appender_folder
from app.loading.data_loader_duckdb import copy_with_update
from utils.time_tracker import track_time

load_dotenv()


@track_time
def extract_boxscore():
    OUTPUT_DIR = "data/landing/boxscore"

    ## CONNECT TO
    db_config = {
        "dbname": os.getenv("PG_DATABASE"),
        "user": os.getenv("PG_USER"),
        "password": os.getenv("PG_PASSWORD"),
        "host": os.getenv("PG_HOST"),
        "port": os.getenv("PG_PORT"),
    }
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM nhl_raw.stg_csv__gameid_to_get")
    results = cursor.fetchall()

    colnames = [desc[0] for desc in cursor.description]

    df = pd.DataFrame(results, columns=colnames)

    cursor.close()
    conn.close()
    games = df["id"]

    for game in games:
        data, _ = make_request(
            f"https://api-web.nhle.com/v1/gamecenter/{game}/boxscore"
        )
        if data:
            save_json(f"raw_{game}_boxcore", data, OUTPUT_DIR)
            print(f"Data collected --- {game}")
        else:
            print(f"Failed --- {game}")


######################################################################################################################################################################


@track_time
def transform_boxscore_games():
    INPUT_DIR = "data/landing/boxscore"
    OUTPUT_DIR = "data/csv_data/staging"  #'data/csv_data/raw/raw_boxscore_game'
    json_dir = os.listdir(INPUT_DIR)

    for file in json_dir:
        j = os.path.join(INPUT_DIR, file)
        try:
            csv_filename = os.path.basename(j)
            csv_filename = csv_filename.replace(".json", ".csv")

            with open(j) as f:
                data = json.load(f)

            parsed_data = pd.json_normalize(data)

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
            parsed_data = parsed_data.drop(
                columns=[col for col in columns_to_drop if col in parsed_data.columns],
                errors="ignore",
            )
            parsed_data["filename"] = csv_filename
            parsed_data.to_csv(f"{OUTPUT_DIR}/{csv_filename}", index=False)
        except Exception as e:
            print(f"Failed to parse {j} --- {e}")


@track_time
def append_boxscore_game():
    today = date.today()
    today = today.strftime("%Y-%m-%d")
    output_file_name = "boxscore_games"
    input_csv_dir = "data/csv_data/staging"
    output_dir = "data/csv_data/processed"
    df_appender_folder(f"{output_file_name}_{today}", input_csv_dir, output_dir)


######################################################################################################################################################################


# @track_time
@track_time
def transform_boxscore_players(file):
    INPUT_DIR = "data/json_data/raw_boxscore"
    OUTPUT_DIR = "csv_data/raw/raw_boxscore_players"
    j = os.path.join(INPUT_DIR, file)

    try:
        csv_filename = os.path.basename(j)
        csv_filename = csv_filename.replace(".json", ".csv")

        with open(j) as f:
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
        parsed_data = pd.concat(
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
        parsed_data = parsed_data.drop(
            columns=[col for col in columns_to_drop if col in parsed_data.columns],
            errors="ignore",
        )
        parsed_data["filename"] = csv_filename
        parsed_data.to_csv(f"{OUTPUT_DIR}/{csv_filename}", index=False)
        print(f"Parsed: {csv_filename}")
    except Exception as e:
        print(f"Something went wrong --- {e}")


@track_time
def append_boxscore_players():
    today = date.today()
    today = today.strftime("%Y-%m-%d")
    output_file_name = "boxscore_players"
    input_csv_dir = "data/csv_data/raw/raw_boxscore_players"  # "data/csv_data/staging"
    output_dir = "data/csv_data/processed/first_loads"
    df_appender_folder(f"{output_file_name}", input_csv_dir, output_dir)


if __name__ == "__main__":
    # extract_boxscore()
    # transform_boxscore_games()
    # append_boxscore_game()

    # Multiprocessing para processar vários arquivos em paralelo

    # with Pool(cpu_count()) as pool:
    #     # Usando apply_async para chamar a função sem argumentos
    #     results = [pool.apply_async(transform_boxscore_players) for _ in range(len(os.listdir('data/json_data/raw_boxscore')))]
    #     for result in results:
    #         result.get()
    # transform_boxscore_players()
    # append_boxscore_players()
    # create_and_load_table('data/csv_data/processed/first_loads/boxscore_players.csv', 'raw_boxscore_players')
    copy_with_update(
        "data/csv_data/processed/boxscore_players_2025-02-05.csv",
        "raw_boxscore_players",
    )
    pass
