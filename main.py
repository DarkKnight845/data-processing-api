from fastapi import FastAPI
import json
import os
from fastapi.responses import FileResponse

app = FastAPI()

@app.get("/")
def read_root():
    return "Welcome to the Polars vs Pandas benchmarking API!"


# @app.get("/benchmark")
# def benchmark():
#     """
#     Benchmarking the loading and aggregation time for Pandas and Polars
#     """
#     import processor.load_data as ld
#     return ld.benchmark()

# @app.get("/clean")
# def clean():
#     """
#     Cleans the data using Pandas and Polars
#     """
#     import processor.clean as cl
#     return {"pandas": cl.clean_pandas(), "polars": cl.clean_polars()}

@app.get("/aggregate")
def aggregate():
    """
    Aggregates the data using Pandas and Polars
    """
    try:
        
        import processor.aggregate as ag
        import pandas as pd
        import polars as pl
        data_url = "https://archive.ics.uci.edu/ml/machine-learning-databases/00502/online_retail_II.xlsx"
        data_pd = pd.read_excel(data_url, sheet_name="Year 2009-2010")
        data_pl = pl.read_excel(data_url, sheet_name="Year 2010-2011")
        pandas_data =  ag.aggregate_pandas(data1=data_pd)
        polars_data = ag.aggregate_polars(data2=data_pl)

        return {
            "pandas": json.loads(pandas_data.to_json(orient="records")) if pandas_data is not None else None,
            "polars": json.loads(polars_data.write_json()) if polars_data is not None else None
            }
    except Exception as e:
        return f"Error while aggregating data: {e}"
    
@app.get("/process-data")
def process_data():
    """
    Cleans and aggregates the data using Pandas and Polars
    """
    try:
        import processor.clean as cl
        import processor.aggregate as ag
        
        # clean data
        pandas_cleaned_data = cl.clean_pandas()
        polars_cleaned_data = cl.clean_polars()
        
        # aggregate data
        pandas_aggregated_data = ag.aggregate_pandas(pandas_cleaned_data)
        polars_aggregated_data = ag.aggregate_polars(polars_cleaned_data)
        
        return {
            "pandas_aggregated_data": json.loads(pandas_aggregated_data.to_json(orient="records")) if pandas_aggregated_data is not None else None,
            "polars_aggregated_data": json.loads(polars_aggregated_data.write_json()) if polars_aggregated_data is not None else None
            }
    except Exception as e:
        return f"Error while processing data: {e}"
    

@app.get("/download-json")
def download_json():
    """
    Download the processed data as a JSON file
    """
    try:
        file_path = "pandas_data.json"

        # check if the file exists
        if not os.path.exists(file_path):
            return f"File not found: {file_path}"
    except Exception as e:
        return f"Error while downloading file: {e}"
    
    return FileResponse(file_path, media_type="application/json", filename="pandas_data.json")
    


@app.get("/download-parquet")
def download_parquet():
    """
    Download the processed data as a Parquet file
    """
    try:
        file_path = "pandas_data.parquet"

        # check if the file exists
        if not os.path.exists(file_path):
            return f"File not found: {file_path}"
    except Exception as e:
        return f"Error while downloading file: {e}"
    
    return FileResponse(file_path, media_type="application/octet-stream", filename="pandas_data.parquet")