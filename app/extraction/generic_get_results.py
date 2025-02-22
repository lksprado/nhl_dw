import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import json
import requests


def make_request(url: str, retries=2):
    """
    Makes a GET request to the provided URL and returns the JSON response.
    Args:
        url (str): The URL to send the GET request to.
    Returns:
        dict | None: The JSON response as a dictionary if the request is successful and returns status code 200.
                    None if the request fails or returns a non-200 status code.
    """
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            return response.json(), response.url
        except requests.exceptions.RequestException as e:
            print(f"Attempt {attempt + 1} failed: {e} - {url}")
            if attempt == retries - 1:
                print(f"All {retries} attempts failed.")
                return None, None


def save_json(file_name: str, data, output_json_dir):
    """
    Saves data to a JSON file.
    Args:
        file_name (str): The name of the file to save the data to.
        data (dict): The data to save in the JSON file / the output from make_request(url)
        output_json_dir (str): The directory to save the JSON file in.
    Obs:
        Adds "raw_" to the beginning of the file name to indicate that the data is raw and has not been processed.
    """
    try:
        file_path = os.path.join(output_json_dir, f"{file_name}.json")
        with open(file_path, "w", encoding="utf-8") as json_file:
            json.dump(data, json_file, indent=4, ensure_ascii=False)
        print(f"JSON saved at: {file_path}")
    except Exception as e:
        print(f"Error on save: {e}")
