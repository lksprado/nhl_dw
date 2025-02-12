import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import pandas as pd
import json
import glob
from datetime import date
from app.transforming.generic_df_appenders import df_appender_folder
from app.loading.data_loader_duckdb import (
    create_and_load_table_with_pk,
    update_table_with_pk,
)


def transform_player_info():
    PATTERN = "data/json_data/raw_player_info/player_*_info.json"
    OUTPUT_DIR = "data/csv_data/raw/raw_player_info/"

    input_files = glob.glob(PATTERN)
    for input_file in input_files:
        try:
            file = os.path.basename(input_file)
            file = os.path.splitext(file)[0]
            file_path = os.path.join(OUTPUT_DIR, file)

            with open(input_file) as f:
                data = json.load(f)
                try:
                    df = pd.json_normalize(data[0])
                except Exception:
                    df = pd.json_normalize(data)

            columns_to_drop = [
                "badges",
                "teamLogo",
                "heroImage",
                "playerSlug",
                "shopLink",
                "twitterLink",
                "watchLink",
                "last5Games" "seasonTotals",
                "currentTeamRoster",
                "fullTeamName.fr",
                "awards",
                "birthStateProvince.fr",
                "firstName.cs",
                "firstName.fi",
                "firstName.sk",
                "lastName.cs",
                "lastName.fi",
                "lastName.sk",
                "birthCity.cs",
                "birthCity.de",
                "birthCity.fi",
                "birthCity.fr",
                "birthCity.sk",
                "birthCity.sv",
                "teamCommonName.fr",
                "birthStateProvince.sk",
                "firstName.de",
                "firstName.es",
                "firstName.sv",
                "lastName.de",
                "lastName.sv",
                "birthStateProvince.sv",
                "lastName.es",
                "firstName.fr",
                "lastName.fr",
                "last5games",
                "seasontotals",
            ]

            df = df.drop(
                columns=[col for col in columns_to_drop if col in df.columns],
                errors="ignore",
            )

            df["filename"] = file
            output_file = file_path + ".csv"
            df.to_csv(output_file, index=False)
            print(f"File saved: {output_file}")
        except Exception as e:
            print(f"Error --- {e}")


def append_player_info():
    today = date.today()
    today = today.strftime("%Y-%m-%d")
    OUTPUT_FILE_NAME = "player_info"
    INPUT_CSV_DIR = "data/csv_data/raw/raw_player_info"
    OUTPUT_DIR = "data/csv_data/processed"
    try:
        appended_file = df_appender_folder(
            f"{OUTPUT_FILE_NAME}_{today}", INPUT_CSV_DIR, OUTPUT_DIR
        )
        print(f"File {OUTPUT_FILE_NAME} created succesfuly in {OUTPUT_DIR}")
        return appended_file
    except FileNotFoundError as e:
        print(f"File or folder not found --- {e}")


def create_player_info(file):
    try:
        create_and_load_table_with_pk(file, "raw_player_info", "playerid")
        print(f"✅ File {file} has been loaded in nhl_raw.raw_player_info")
        return file
    except Exception as e:
        print(f"Failed to load {file} --- {e}")


def load_player_info(file):
    try:
        update_table_with_pk(file, "raw_player_info", "playerid")
        print(f"✅ File {file} has been loaded in nhl_raw.raw_player_info")
        return file
    except Exception as e:
        print(f"Failed to load {file} --- {e}")


if __name__ == "__main__":
    transform_player_info()
    appended_file = append_player_info()

    if appended_file:
        loaded_file = create_player_info(appended_file)
    pass
