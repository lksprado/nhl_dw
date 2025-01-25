import os
import pandas as pd
from multiprocessing import Pool, cpu_count


def df_appender_file(filename):
    if os.path.getsize(filename) > 0:  # Verifica se o arquivo não está vazio
        try:
            df = pd.read_csv(filename, index_col=None, header=0)
            return df
        except pd.errors.EmptyDataError:
            print(f"Arquivo vazio ou sem colunas: {filename}")
            return None
    else:
        print(f"Arquivo vazio: {filename}")
        return None


def df_appender_folder(output_file_name, input_csv_dir, output_dir):
    all_files = [
        os.path.join(input_csv_dir, f)
        for f in os.listdir(input_csv_dir)
        if f.endswith(".csv")
    ]

    with Pool(cpu_count()) as pool:
        dfs = pool.map(df_appender_file, all_files)

    # Filtra os DataFrames válidos (não None)
    dfs = [df for df in dfs if df is not None]

    if dfs:
        frame = pd.concat(dfs, axis=0, ignore_index=True)
        output_path = os.path.join(output_dir, f"{output_file_name}.csv")
        frame.to_csv(output_path, index=False)
        print(f"Arquivo salvo em: {output_path}")
    else:
        print("Nenhum arquivo válido para processar.")
