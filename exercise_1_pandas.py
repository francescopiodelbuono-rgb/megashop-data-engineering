import pandas as pd
import glob
import os

percorso = "data_local/json/*.jsonl"

files = glob.glob(percorso)

print("File trovati:", len(files))

totale_generale = 0

for file in files:
    df = pd.read_json(file, lines=True)

    totale_file = df["amount"].sum()

    totale_generale += totale_file

    print(os.path.basename(file), totale_file)

print("\nFATTURATO TOTALE:", totale_generale)