import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import pandas as pd
import polars as pl
from tqdm import tqdm

from app.loading.generic_data_loader import PostgresClient
from utils.time_tracker import track_time

fn = "raw_"


class DataLoader:
    def __init__(self):
        self.db = PostgresClient()

    def load_csv_to_db(self, csv_directory):
        for root, _, files in os.walk(csv_directory):
            for file in files:
                if file.endswith(".csv"):
                    file_path = os.path.join(root, file)
                    table_name = fn + os.path.splitext(file)[0]
                    df = pd.read_csv(file_path, dtype=str)  # Specify dtype as str
                    self.db.save_dataframe(df, table_name)
        print("Done")

    @track_time
    def load_csv_to_single_polars(self, file_path):
        file = os.path.basename(file_path)
        table_name = fn + os.path.splitext(file)[0]
        with tqdm(
            total=os.path.getsize(file_path),
            unit="B",
            unit_scale=True,
            desc="Reading CSV",
        ) as pbar:
            df = pl.read_csv(
                file_path,
                schema_overrides={
                    col: pl.Utf8 for col in pl.read_csv(file_path, n_rows=0).columns
                },
            )
            pbar.update(os.path.getsize(file_path))
        self.db.save_dataframe_polars(df, table_name)
        print("Done")

    def load_csv_to_single_pandas(self, file_path):
        file = os.path.basename(file_path)
        table_name = fn + os.path.splitext(file)[0]
        df = pd.read_csv(file_path, dtype=str)  # Specify dtype as str
        self.db.save_dataframe(df, table_name)
        print("Done")


if __name__ == "__main__":
    dl = DataLoader()
    # dl.load_csv_to_db('data/csv_data/processed')

    dl.load_csv_to_single_pandas("data/csv_data/processed/teams.csv")
