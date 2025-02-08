import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))


import pandas as pd
import json
import glob

from app.extraction.generic_get_results import make_request, save_json
from app.transforming.generic_df_appenders import df_appender_folder
from app.loading.data_loader_duckdb import truncate_and_load_table


## raw_all_goalies_stats
def extract_goalie_stats():
    """
    #### Daily Update
    Response: JSON format
    """
    URL = "https://api.nhle.com/stats/rest/en/goalie/summary?limit=-1&cayenneExp=seasonId={season_id}"
    OUTPUT_DIR = "data/json_data/raw_all_goalies_stats"
    PARAMETER_FILE = "app/api_parameters/season_ids.csv"

    df_parameter = pd.read_csv(PARAMETER_FILE)

    ## TO GET RECENT
    max_season_id = df_parameter["season_id"].max()
    url = URL.format(season_id=max_season_id)
    data, _ = make_request(url)
    save_json(f"raw_stats_all_goalies_{max_season_id}", data, OUTPUT_DIR)

    ## TO GET HISTORIC DATA
    # urls = [URL.format(season_id=row.season_id) for row in df_parameter.itertuples()]
    # for url, row in zip(urls, df_parameter.itertuples()):
    #     season_id = row.season_id
    #     data = make_request(url)
    #     save_json(f"raw_stats_all_goalies_{season_id}", data, OUTPUT_DIR)
    #     print(f'Saving: raw_stats_all_goalies_{season_id}')


def transform_goalie_stats():
    PATTERN = "data/json_data/raw_all_goalies_stats/raw_stats_all_goalies_*.json"
    OUTPUT_DIR = "data/csv_data/raw/raw_all_goalies_stats"
    input_files = glob.glob(PATTERN)
    for input_file in input_files:
        file = os.path.basename(input_file)
        file = os.path.splitext(file)[0]
        file_path = os.path.join(OUTPUT_DIR, file)
        with open(input_file) as f:
            data = json.load(f)

        ## NORMALIZADO EM UMA LINHA
        df = pd.json_normalize(data[0], record_path=["data"], errors="ignore")
        df["filename"] = file

        output_file = file_path + ".csv"
        df.to_csv(output_file, index=False)
        print(f"Arquivo salvo: {output_file}")


def append_goalies_stats():
    OUTPUT_FILENAME = "all_goalies_stats"
    INPUT_CSV_DIR = "data/csv_data/raw/raw_all_goalies_stats"
    OUTPUT_DIR = "data/csv_data/processed"
    df_appender_folder(OUTPUT_FILENAME, INPUT_CSV_DIR, OUTPUT_DIR)


def load_goalies_stats():
    truncate_and_load_table(
        "data/csv_data/processed/all_goalies_stats.csv", "raw_all_goalies_stats"
    )


if __name__ == "__main__":
    # extract_goalie_stats()
    transform_goalie_stats()
    append_goalies_stats()
    load_goalies_stats()
    pass
