import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import glob
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import json
from app.extraction.generic_get_results import make_request, save_json
from app.transforming.generic_df_appenders import df_appender_folder
from app.loading.data_loader_duckdb import create_and_load_table


def fetch_and_save_club_stats(team_id, season_id, game_type, url, output_dir):
    data, _ = make_request(url)
    if data:
        save_json(f"raw_stats_club_{team_id}_{season_id}_{game_type}", data, output_dir)
        print(f"Data collected --- {url}")
    else:
        print(f"Failed --- {url}")


def extract_club_stats_historic():
    """
    #### Daily Update
    """

    URL = "https://api-web.nhle.com/v1/club-stats/{team_id}/{season_id}/{game_type}"
    OUTPUT_DIR = "data/json_data/raw_club_stats"

    parameters_input = "app/api_parameters/seasonid_teamid_game_type.csv"
    df_parameter = pd.read_csv(parameters_input)
    df_parameter = df_parameter.sort_values(
        by=["season_id", "team_id", "game_type"], ascending=False
    )

    # max_season_id = df_parameter["season_id"].max()
    # df_filtered = df_parameter[df_parameter["season_id"] == max_season_id]
    # unique_teams = df_filtered["team_id"].unique()
    # for team_id in unique_teams:
    #     url = URL.format(
    #         team_id=team_id, season_id=max_season_id, season_step=season_type
    #     )
    #     data, _ = make_request(url)
    #     save_json(
    #         f"stats_club_{team_id}_{max_season_id}_{season_type}", data, OUTPUT_DIR
    #     )

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for row in df_parameter.itertuples():
            team_id = row.team_id
            season_id = row.season_id
            game_type_id = row.game_type
            url = URL.format(
                team_id=team_id, season_id=season_id, game_type=game_type_id
            )
            futures.append(
                executor.submit(
                    fetch_and_save_club_stats,
                    team_id,
                    season_id,
                    game_type_id,
                    url,
                    OUTPUT_DIR,
                )
            )

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Exception occurred: {e}")


def transform_club_stats():
    PATTERN = "data/json_data/raw_club_stats/raw_stats_club_*_*_*.json"
    OUTPUT_DIR = "data/csv_data/raw/raw_club_stats"
    input_files = glob.glob(PATTERN)
    for input_file in input_files:
        file = os.path.basename(input_file)
        file = os.path.splitext(file)[0]
        file_path = os.path.join(OUTPUT_DIR, file)
        with open(input_file) as f:
            data = json.load(f)

        ## NORMALIZADO EM UMA LINHA
        df_skaters = pd.json_normalize(
            data, meta=["season", "gameType"], record_path=["skaters"], errors="ignore"
        )
        df_skaters["filename"] = file

        df_goalies = pd.json_normalize(
            data, meta=["season", "gameType"], record_path=["goalies"], errors="ignore"
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
        print(f"Arquivo salvo: {output_file}")


def append_club_stats():
    output_file_name = "all_club_stats"
    input_csv_dir = "data/csv_data/raw/raw_club_stats"
    output_dir = "data/csv_data/processed"
    df_appender_folder(output_file_name, input_csv_dir, output_dir)


def load_club_stats():
    create_and_load_table(
        "data/csv_data/processed/all_club_stats.csv", "raw_all_club_stats"
    )


if __name__ == "__main__":
    # extract_club_stats_historic()
    # transform_club_stats()
    # append_club_stats()
    load_club_stats()
