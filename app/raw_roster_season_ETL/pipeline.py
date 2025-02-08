import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import pandas as pd
import json
from app.extraction.generic_get_results import make_request, save_json
from app.transforming.generic_df_appenders import df_appender_folder
from app.loading.data_loader_duckdb import update_table_with_sk


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
        team_id = url.split("/")[-2]
        season_id = url.split("/")[-1]
        file_name = f"raw_roster_{team_id}_{season_id}"
        data, _ = make_request(url)

        save_json(file_name, data, OUTPUT_DIR)
        print(f"Data Collect --- {file_name}")


def transform_team_roster_by_season():
    INPUT_DIR = "data/json_data/raw_roster_season/landing"
    OUTPUT_DIR = "data/csv_data/raw/raw_roster_season/staging"

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
            df["filename"] = file_name

            # Salvar como CSV
            df.to_csv(output_path, index=False)
            print(f"✅ Arquivo processado: {input_file} → {file_name}.csv")

        except Exception as e:
            print(f"❌ Erro ao processar {input_file}: {e}")


def append_roster_season():
    output_file_name = "roster_season"
    input_csv_dir = "data/csv_data/raw/raw_roster_season/staging"
    output_dir = "data/csv_data/processed"
    df_appender_folder(output_file_name, input_csv_dir, output_dir)


def load_roster_season():
    # create_and_load_table_with_sk('data/csv_data/processed/first_loads/roster_season.csv','raw_roster_season','id')
    update_table_with_sk(
        "/media/lucas/Files/2.Projetos/nhl-dw/data/csv_data/processed/roster_season.csv",
        "raw_roster_season",
        "id",
    )


if __name__ == "__main__":
    # extract_team_roster_by_season()
    # transform_team_roster_by_season()
    # append_roster_season()
    load_roster_season()
    pass
