import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
import json

import pandas as pd
import polars as pl

from utils.time_tracker import track_time


@track_time
def parsing_json_pandas(filename: str, ls, output_dir: str):
    """
    Parses json and saves as csv using Pandas lib \n
    filename:
        Provide the json file
    ls:
        Provide desired json list element
    output_dir:
        Provide output folder for csv file
    """
    csv_filename = os.path.basename(filename)
    csv_filename = csv_filename.replace(".json", ".csv")
    data = pd.read_json(filename)
    if isinstance(data, pd.DataFrame):
        parsed_data = data
    elif isinstance(data, pd.Series):
        parsed_data = pd.json_normalize(data[ls])

    parsed_data.to_csv(f"{output_dir}/{csv_filename}", index=False)


# @track_time
def parsing_json_pandas_2(filename: str, output_dir: str):
    """
    Parses json and saves as csv using Pandas lib
    filename:
        Provide the json file
    output_dir:
        Provide output folder for csv file
    """
    csv_filename = os.path.basename(filename)
    csv_filename = csv_filename.replace(".json", ".csv")
    file_id = csv_filename.replace(".csv", "")
    try:
        with open(filename) as json_data:
            data = json.load(json_data)
    except ValueError as e:
        print(f"Error reading JSON file {filename}: {e}")
        return

    all_stats = []

    if isinstance(data, dict):
        # Elementos que não são dicionários
        non_dict_elements = {
            key: value for key, value in data.items() if not isinstance(value, list)
        }

        # Processar elementos que são listas de dicionários
        for key, value in data.items():
            if isinstance(value, list):
                try:
                    stats = pd.json_normalize(value)
                    for col, val in non_dict_elements.items():
                        stats[col] = val
                    stats["stats"] = key
                    stats["filename"] = file_id
                    all_stats.append(stats)
                except Exception as e:
                    print(f"Erro ao processar posição {key}: {e}")
    elif isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                non_dict_elements = {
                    key: value
                    for key, value in item.items()
                    if not isinstance(value, list)
                }
                for key, value in item.items():
                    if isinstance(value, list):
                        try:
                            stats = pd.json_normalize(value)
                            for col, val in non_dict_elements.items():
                                stats[col] = val
                            stats["stats"] = key
                            stats["filename"] = file_id
                            all_stats.append(stats)
                        except Exception as e:
                            print(f"Erro ao processar posição {key}: {e}")

    if all_stats:
        # Concatenar todos os DataFrames em um único DataFrame
        parsed_data = pd.concat(all_stats, ignore_index=True, sort=True)
    else:
        raise ValueError("No player data found in the JSON file")

    parsed_data.to_csv(f"{output_dir}/{csv_filename}", index=False)


@track_time
def parsing_json_pandas_3(filename: str, output_dir: str):
    file = os.path.basename(filename)
    file = os.path.splitext(file)[0]
    file_path = os.path.join(output_dir, file)

    with open(filename) as f:
        data = json.load(f)
        data = data[0]
        df = pd.json_normalize(data)
    df.to_csv(file_path + ".csv", index=False)
    print("Done")


@track_time
def parsing_json_polars(filename: str, ls: str, output_dir: str):
    """
    Parses json and saves as csv using Polars lib
    filename:
        Provide the json file
    ls:
        Provide desired json list element
    output_dir:
        Provide output folder for csv file
    """
    csv_filename = os.path.basename(filename)
    csv_filename = csv_filename.replace(".json", ".csv")

    data = pl.read_json(filename)
    df = pl.DataFrame(data[ls]).explode(ls)

    def unnest_all(df, separator="."):
        def _unnest_all(struct_columns):
            return df.with_columns(
                [
                    pl.col(col).struct.rename_fields(
                        [
                            f"{col}{separator}{field_name}"
                            for field_name in df[col].struct.fields
                        ]
                    )
                    for col in struct_columns
                ]
            ).unnest(struct_columns)

        struct_columns = [col for col in df.columns if df[col].dtype == pl.Struct]
        while len(struct_columns):
            df = _unnest_all(struct_columns=struct_columns)
            struct_columns = [col for col in df.columns if df[col].dtype == pl.Struct]
        return df

    # Desanexar as estruturas
    df_unnested = unnest_all(df)
    df_unnested.columns = [col.replace(f"{ls}.", "") for col in df_unnested.columns]
    df_unnested.write_csv(f"{output_dir}/{csv_filename}")
