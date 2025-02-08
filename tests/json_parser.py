import pandas as pd
import json
import os

input_file = "tests/raw_stats_club_ANA_20182019_2.json"
file = os.path.basename(input_file)
file = os.path.splitext(file)[0]
file_path = os.path.join("tests", file)
with open(input_file) as f:
    data = json.load(f)

df = pd.json_normalize(
    data, meta=["season", "gameType"], record_path=["goalies"], errors="ignore"
)
df["filename"] = file

# drop = ["lastName.cs",
#         "firstName.cs",
#         "firstName.sk",
#         "lastName.sk",
#         "firstName.de",
#         "firstName.es",
#         "firstName.fi",
#         "firstName.sv",
#         "lastName.fi",
#         "lastName.sv"
#     ]
# df = df.drop(columns=drop)

output_file = file_path + "_goalies" + ".csv"
df.to_csv(output_file, index=False)
print(f"Arquivo salvo: {output_file}")
