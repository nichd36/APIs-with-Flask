import os
import pandas as pd
# import pyarrow.parquet as pq
import urllib

from flask import Flask, jsonify
app = Flask(__name__)

@app.route("/api/python")
def hello_world():
    return "<p>Hello, World!</p>"

@app.route("/api/process_parquet")
def process():
    filename = '/tmp/prasarana_timeseries.parquet'

    if not os.path.exists(filename):
        print(f"{filename} not found. Fetching...")
        url = "https://storage.data.gov.my/dashboards/prasarana_timeseries.parquet"
        urllib.request.urlretrieve(url, filename)
    else:
        print(f"{filename} exists.")


    df = pd.read_parquet(filename, engine='fastparquet')

    grouped_df = df.groupby('date', as_index=False)['passengers'].sum()

    result = grouped_df.head().to_dict(orient='records')

    return jsonify(result)