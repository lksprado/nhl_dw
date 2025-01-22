from app.loading.generic_data_loader import PostgresClient
import os
import pandas as pd

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

    def load_csv_to_single(self, file_path):
        file = os.path.basename(file_path)
        table_name = fn + os.path.splitext(file)[0]
        df = pd.read_csv(file_path, dtype=str)  # Specify dtype as str
        self.db.save_dataframe(df, table_name)
        print("Done")


if __name__ == "__main__":
    dl = DataLoader()
    # dl.load_csv_to_db('data/csv_data/processed')
    dl.load_csv_to_single("data/csv_data/processed/player_info.csv")
