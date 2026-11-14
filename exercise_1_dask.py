import dask.dataframe as dd

percorso = "data_local/json/*.jsonl"

df = dd.read_json(percorso, lines=True)

risultato = df.groupby("payment_type")["amount"].mean().compute()

print(risultato)