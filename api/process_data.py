import os
import subprocess
import urllib.request
import pyarrow.parquet as pq
import pandas as pd
from flask import Flask
app = Flask(__name__)

@app.route("/api/process_parquet")
def process():
    filename = 'prasarana_timeseries.parquet'

    if not os.path.exists(filename):
        print(f"{filename} not found. Fetching...")
        url = "https://storage.data.gov.my/dashboards/prasarana_timeseries.parquet"
        urllib.request.urlretrieve(url, 'prasarana_timeseries.parquet')
    else:
        print(f"{filename} exists.")

    data = pq.read_table(filename)
    df = data.to_pandas() 

    grouped_df = df.groupby('date', as_index=False)['passengers'].sum()

    return(grouped_df.head())