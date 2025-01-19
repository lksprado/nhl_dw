import glob
import os


def rename_files(directory):
    pattern = os.path.join(directory, "*.json.json")
    files = glob.glob(pattern)

    for file in files:
        new_name = file.replace(".json.json", ".json")
        os.rename(file, new_name)
        print(f"Renamed: {file} -> {new_name}")


if __name__ == "__main__":
    directory = "data/json_data/raw_roster_season"
    rename_files(directory)
