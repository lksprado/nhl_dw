import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
import glob
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import json
from app.extraction.generic_get_results import make_request, save_json
from app.transforming.generic_df_appenders import df_appender_folder
from app.loading.data_loader_duckdb import update_table_with_pk
from datetime import date


## raw_player_info
def fetch_and_save_player_info(player_id, url, output_dir):
    data, _ = make_request(url)
    if data:
        save_json(f"player_{player_id}_info", data, output_dir)
        print(f"Data collected --- {url}")
    else:
        print(f"Failed --- {url}")


## raw_player_info
def extract_player_info():
    """
    #### Daily Update
    """
    URL = "https://api-web.nhle.com/v1/player/{player_id}/landing"
    OUTPUT_DIR = "data/json_data/raw_player_info/landing"

    parameters_input = "app/api_parameters/active_playerid.csv"
    df_parameter = pd.read_csv(parameters_input)

    pattern = os.path.join(OUTPUT_DIR, "player_*_info.json")
    files_to_skip = glob.glob(pattern)
    existing_combinations = set()
    for file in files_to_skip:
        match = re.search(r"player_(\d+)_info\.json", os.path.basename(file))
        if match:
            player_id = match.group(1)
            existing_combinations.add((player_id))

    df_filtered = df_parameter[
        ~df_parameter.apply(
            lambda row: str(row["playerid"]) in existing_combinations, axis=1
        )
    ]

    df_filtered = df_filtered["playerid"].unique()
    with ThreadPoolExecutor(max_workers=15) as executor:
        futures = []
        for player_id in df_filtered:
            url = URL.format(player_id=player_id)
            futures.append(
                executor.submit(fetch_and_save_player_info, player_id, url, OUTPUT_DIR)
            )

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Exception occurred: {e}")


def transform_player_info():
    INPUT_DIR = "data/json_data/raw_player_info/landing"
    OUTPUT_DIR = "data/csv_data/raw/raw_player_info/staging"

    input_files = [f for f in os.listdir(INPUT_DIR) if f.endswith(".json")]

    for input_file in input_files:
        try:
            # Caminho completo do arquivo JSON
            input_path = os.path.join(INPUT_DIR, input_file)

            # Nome do arquivo sem extensão para gerar CSV correspondente
            file_name = os.path.splitext(input_file)[0]
            output_path = os.path.join(OUTPUT_DIR, file_name + ".csv")

            # Ler JSON e normalizar
            with open(input_path, encoding="utf-8") as f:
                data = json.load(f)
                df = pd.json_normalize(data)  # Assumindo que data[0] sempre existe

            # Salvar como CSV
            df.to_csv(output_path, index=False)
            print(f"✅ Arquivo processado: {input_file} → {file_name}.csv")

        except Exception as e:
            print(f"❌ Erro ao processar {input_file}: {e}")


def append_player_info():
    today = date.today()
    today = today.strftime("%Y-%m-%d")
    output_file_name = "player_info"
    input_csv_dir = "data/csv_data/raw/raw_player_info/staging"
    output_dir = "data/csv_data/processed"
    df_appender_folder(f"{output_file_name}_{today}", input_csv_dir, output_dir)


def load_player_info():
    # create_and_load_table_with_pk('data/csv_data/processed/first_loads/player_info.csv', 'raw_player_info','playerid')
    update_table_with_pk(
        "data/csv_data/processed/player_info_2025-02-08.csv",
        "raw_player_info",
        "playerid",
    )


if __name__ == "__main__":
    # extract_player_info()
    # transform_player_info()
    # append_player_info()
    load_player_info()
    pass
