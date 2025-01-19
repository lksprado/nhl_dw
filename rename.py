import glob
import os


def rename_files(directory):
    pattern = os.path.join(directory, "*.json")
    files = glob.glob(pattern)

    for file in files:
        new_name = file.replace("_now", "")
        os.rename(file, new_name)
        print(f"Renamed: {file} -> {new_name}")


if __name__ == "__main__":
    directory = "data/csv_data/raw/raw_club_status"
    rename_files(directory)
