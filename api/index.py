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
    filename = 'prasarana_timeseries.parquet'

    if not os.path.exists(filename):
        print(f"{filename} not found. Fetching...")
        url = "https://storage.data.gov.my/dashboards/prasarana_timeseries.parquet"
        urllib.request.urlretrieve(url, 'prasarana_timeseries.parquet')
    else:
        print(f"{filename} exists.")

    # data = pq.read_table(filename)
    # df = data.to_pandas() 

    df = pd.read_parquet('prasarana_timeseries.parquet', engine='fastparquet')

    grouped_df = df.groupby('date', as_index=False)['passengers'].sum()

    result = grouped_df.head().to_dict(orient='records')

    return jsonify(result)