import pandas as pd
import os
from utils.time_tracker import track_time
from multiprocessing import Pool, cpu_count


def process_file(file_path, output_folder):
    try:
        file_name = os.path.basename(file_path).replace(".csv", "")
        df = pd.read_csv(file_path, dtype=str)
        df["filename"] = file_name
        output_file_path = os.path.join(output_folder, os.path.basename(file_path))
        df.to_csv(output_file_path, index=False)
        print(f"Arquivo salvo em: {output_file_path}")
    except Exception as e:
        print(f"Erro ao processar {file_path}: {e}")


@track_time
def insert_filename():
    input_folder = "data/csv_data/raw/raw_play_by_play"
    output_folder = "data/csv_data/processed"

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    input_files = [
        os.path.join(input_folder, file)
        for file in os.listdir(input_folder)
        if file.endswith(".csv")
    ]

    with Pool(cpu_count()) as pool:
        pool.starmap(process_file, [(file, output_folder) for file in input_files])


if __name__ == "__main__":
    insert_filename()
