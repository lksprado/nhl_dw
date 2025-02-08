import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import pandas as pd
import json
import glob
from app.extraction.generic_get_results import make_request, save_json
from app.transforming.generic_df_appenders import df_appender_folder
from app.loading.data_loader_duckdb import create_and_load_table


## raw_all_skaters_stats
def extract_skater_stats():
    """
    #### Daily Update
    Response: JSON format
    """
    URL = "https://api.nhle.com/stats/rest/en/skater/summary?limit=-1&cayenneExp=seasonId={season_id}"
    OUTPUT_DIR = "data/json_data/raw_all_skaters_stats"
    PARAMETER_FILE = "app/api_parameters/season_ids.csv"

    df_parameter = pd.read_csv(PARAMETER_FILE)

    ## TO GET RECENT
    max_season_id = df_parameter["season_id"].max()
    url = URL.format(season_id=max_season_id)
    data, _ = make_request(url)
    save_json(f"raw_stats_all_skaters_{max_season_id}", data, OUTPUT_DIR)

    ## TO GET HISTORIC DATA
    # urls = [URL.format(season_id=row.season_id) for row in df_parameter.itertuples()]
    # for url, row in zip(urls, df_parameter.itertuples()):
    #     season_id = row.season_id
    #     data = make_request(url)
    #     save_json(f"raw_stats_all_skaters_{season_id}", data, OUTPUT_DIR)
    #     print(f'Saving: raw_stats_all_skaters_{season_id}')


def transform_skater_stats():
    PATTERN = "data/json_data/raw_all_skaters_stats/raw_stats_all_skaters_*.json"
    OUTPUT_DIR = "data/csv_data/raw/raw_all_skaters_stats"
    input_files = glob.glob(PATTERN)
    for input_file in input_files:
        try:
            file = os.path.basename(input_file)
            file = os.path.splitext(file)[0]
            file_path = os.path.join(OUTPUT_DIR, file)
            with open(input_file) as f:
                data = json.load(f)
            try:
                ## NORMALIZADO EM UMA LINHA
                df = pd.json_normalize(data[0], record_path=["data"], errors="ignore")
            except Exception:
                df = pd.json_normalize(data, record_path=["data"], errors="ignore")

            df["filename"] = file
            output_file = file_path + ".csv"
            df.to_csv(output_file, index=False)
            print(f"Arquivo salvo: {output_file}")
        except Exception as e:
            print(f"Error --- {e} --- file: {output_file}")


def append_skaters_stats():
    OUTPUT_FILENAME = "all_skaters_stats"
    INPUT_CSV_DIR = "data/csv_data/raw/raw_all_skaters_stats"
    OUTPUT_DIR = "data/csv_data/processed"
    df_appender_folder(OUTPUT_FILENAME, INPUT_CSV_DIR, OUTPUT_DIR)


def load_skaters_stats():
    create_and_load_table(
        "data/csv_data/processed/all_skaters_stats.csv", "raw_all_skaters_stats"
    )


if __name__ == "__main__":
    # extract_skater_stats()
    # transform_skater_stats()
    # append_skaters_stats()
    # load_skaters_stats()
    pass
