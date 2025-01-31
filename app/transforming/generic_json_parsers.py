# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
import json
import os

import pandas as pd
import polars as pl

from utils.time_tracker import track_time


@track_time
def parsing_json_pandas(filename: str, ls, output_dir: str):
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
def parsing_json_pandas_4(filename: str, output_dir: str):
    file = os.path.basename(filename)
    file = os.path.splitext(file)[0]
    file_path = os.path.join(output_dir, file)

    with open(filename) as f:
        data = json.load(f)
        # Inicializando o DataFrame vazio
        df = pd.DataFrame()
        # Normalizando os dados de "plays" (caso exista)
        if "plays" in data:
            df = pd.json_normalize(data, record_path=["plays"])
        # Extraindo e expandindo as colunas de awayTeam (caso exista)
        if "awayTeam" in data:
            away_team = pd.json_normalize(data["awayTeam"])
            for col in away_team.columns:
                df[f"awayTeam.{col}"] = away_team.loc[0, col]
        # Extraindo e expandindo as colunas de homeTeam (caso exista)
        if "homeTeam" in data:
            home_team = pd.json_normalize(data["homeTeam"])
            for col in home_team.columns:
                df[f"homeTeam.{col}"] = home_team.loc[0, col]
        # Adicionando o nome do arquivo como referência
        if not df.empty:
            df["filename"] = file
    # Verificando se o DataFrame contém colunas antes de tentar removê-las
    columns_to_drop = [
        "details.highlightClipSharingUrlFr",
        "details.highlightClip",
        "details.highlightClipFr",
        "details.discreteClip",
        "details.discreteClipFr",
        "awayTeam.logo",
        "awayTeam.darkLogo",
        "awayTeam.placeNameWithPreposition.default",
        "awayTeam.placeNameWithPreposition.fr",
        "homeTeam.logo",
        "homeTeam.darkLogo",
        "homeTeam.placeNameWithPreposition.default",
        "homeTeam.placeNameWithPreposition.fr",
        "hometeam_commonname_fr",
        "awayteam_commonname_fr",
        "awayteam_placename_fr",
        "awayteam_placename_fr",
    ]
    existing_columns = [col for col in columns_to_drop if col in df.columns]
    if existing_columns:
        df = df.drop(columns=existing_columns)
    # Salvando o DataFrame como CSV, apenas se não estiver vazio
    if not df.empty:
        df.to_csv(file_path + ".csv", index=False)
        print(f"Arquivo criado com sucesso: {file_path}.csv")
    else:
        print(f"Nenhum dado relevante encontrado em {filename}. Arquivo não criado.")


@track_time
def parsing_json_pandas_5(input_file, output_path):
    file = os.path.basename(input_file)
    file = os.path.splitext(file)[0]
    file_path = os.path.join(output_path, file)

    with open(input_file) as f:
        data = json.load(f)
        df = pd.json_normalize(data["data"])
    df.to_csv(file_path + ".csv", index=False)


def parsing_json_pandas_6(filename: str, output_dir: str):
    # Extrai o nome base do arquivo sem extensão
    file = os.path.basename(filename)
    file = os.path.splitext(file)[0]
    file_path = os.path.join(output_dir, file)
    with open(filename) as f:
        data = json.load(f)

    ## NORMALIZADO EM UMA LINHA
    df_refs = pd.json_normalize(
        data,
        record_path=["gameInfo", "referees"],
        record_prefix="gameInfo.referees.",
        errors="ignore",
    )
    df_refs["filename"] = file
    df_refs["referee_index"] = df_refs.groupby("filename").cumcount() + 1
    df_refs = df_refs.pivot(
        index="filename", columns="referee_index", values="gameInfo.referees.default"
    )
    df_refs.columns = [f"referee_{col}" for col in df_refs.columns]

    # NORMALIZADO EM UMA LINHA
    df_final_score = pd.json_normalize(data)
    df_final_score = df_final_score[["linescore.totals.away", "linescore.totals.home"]]
    df_final_score = df_final_score.rename(
        columns={
            "linescore.totals.away": "final_score_away",
            "linescore.totals.home": "final_score_home",
        }
    )
    df_final_score["filename"] = file

    ## NORMALIZADO EM UMA LINHA
    df_period_score = pd.json_normalize(
        data,
        record_path=["linescore", "byPeriod"],
        record_prefix="linescore.byPeriod.",
        errors="ignore",
    )
    df_home = df_period_score[
        ["linescore.byPeriod.home", "linescore.byPeriod.periodDescriptor.number"]
    ]
    df_home = df_home.rename(
        columns={
            "linescore.byPeriod.home": "score_home",
            "linescore.byPeriod.periodDescriptor.number": "period",
        }
    )
    df_home["filename"] = file
    df_away = df_period_score[
        ["linescore.byPeriod.away", "linescore.byPeriod.periodDescriptor.number"]
    ]
    df_away = df_away.rename(
        columns={
            "linescore.byPeriod.away": "score_away",
            "linescore.byPeriod.periodDescriptor.number": "period",
        }
    )
    df_away["filename"] = file
    df_home = df_home.pivot(index="filename", columns="period", values="score_home")
    df_away = df_away.pivot(index="filename", columns="period", values="score_away")
    df_home.columns = [f"score_home_period_{col}" for col in df_home.columns]
    df_away.columns = [f"score_away_period_{col}" for col in df_away.columns]
    df_period_score = pd.concat([df_home, df_away], axis=1).reset_index()

    # NORMALIZADO EM UMA LINHA
    df_period_shots = pd.json_normalize(
        data,
        record_path=["shotsByPeriod"],
        record_prefix="shotsByPeriod.",
        errors="ignore",
    )
    df_home_shots = df_period_shots[
        ["shotsByPeriod.home", "shotsByPeriod.periodDescriptor.number"]
    ]
    df_home_shots = df_home_shots.rename(
        columns={
            "shotsByPeriod.home": "shots_home",
            "shotsByPeriod.periodDescriptor.number": "period",
        }
    )
    df_away_shots = df_period_shots[
        ["shotsByPeriod.away", "shotsByPeriod.periodDescriptor.number"]
    ]
    df_away_shots = df_away_shots.rename(
        columns={
            "shotsByPeriod.away": "shots_away",
            "shotsByPeriod.periodDescriptor.number": "period",
        }
    )
    df_home_shots["filename"] = file
    df_home_shots = df_home_shots.pivot(
        index="filename", columns="period", values="shots_home"
    )
    df_away_shots["filename"] = file
    df_away_shots = df_away_shots.pivot(
        index="filename", columns="period", values="shots_away"
    )
    df_home_shots.columns = [
        f"shots_home_period_{col}" for col in df_home_shots.columns
    ]
    df_away_shots.columns = [
        f"shots_away_period_{col}" for col in df_away_shots.columns
    ]
    df_period_shots = pd.concat([df_home_shots, df_away_shots], axis=1).reset_index()
    df_period_shots["filename"] = file

    # NORMALIZADO EM UMA LINHA
    df_stats = pd.json_normalize(
        data,
        record_path=["teamGameStats"],
    )
    df_stats = pd.DataFrame(
        {
            f"away_{row['category']}": [row["awayValue"]]
            for _, row in df_stats.iterrows()
        }
    ).join(
        pd.DataFrame(
            {
                f"home_{row['category']}": [row["homeValue"]]
                for _, row in df_stats.iterrows()
            }
        )
    )
    df_stats["filename"] = file

    df = (
        df_refs.merge(df_final_score, on="filename", how="outer")
        .merge(df_period_score, on="filename", how="outer")
        .merge(df_period_shots, on="filename", how="outer")
        .merge(df_stats, on="filename", how="outer")
    )

    output_file = file_path + ".csv"
    df.to_csv(output_file, index=False)
    print(f"Arquivo salvo: {output_file}")


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


def parsing_json_pandas_7(filename: str, output_dir: str):
    csv_filename = os.path.basename(filename)
    csv_filename = csv_filename.replace(".json", ".csv")

    with open(filename) as f:
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
    columns_to_drop = ["name.cs", "name.fi", "name.sk", "name.sv"]
    parsed_data = parsed_data.drop(
        columns=[col for col in columns_to_drop if col in parsed_data.columns],
        errors="ignore",
    )
    parsed_data["filename"] = csv_filename
    parsed_data.to_csv(f"{output_dir}/{csv_filename}", index=False)


def parsing_json_pandas_8(filename: str, output_dir: str):
    pd.set_option("display.max_columns", None)
    pd.set_option("display.max_rows", 3)

    csv_filename = os.path.basename(filename)
    csv_filename = csv_filename.replace(".json", ".csv")

    with open(filename) as f:
        data = json.load(f)

    parsed_data = pd.json_normalize(data)

    # Drop unnecessary columns
    columns_to_drop = [
        "tvBroadcasts",
        "awayTeam.logo",
        "awayTeam.darkLogo",
        "awayTeam.placeNameWithPreposition.fr",
        "homeTeam.commonName.fr",
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
    ]
    parsed_data = parsed_data.drop(
        columns=[col for col in columns_to_drop if col in parsed_data.columns],
        errors="ignore",
    )
    parsed_data["filename"] = csv_filename
    parsed_data.to_csv(f"{output_dir}/{csv_filename}", index=False)
