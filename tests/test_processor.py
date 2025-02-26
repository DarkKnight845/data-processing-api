import pytest
import pandas as pd
import polars as pl
from processor.aggregate import aggregate_pandas, aggregate_polars
from processor.clean import clean_pandas, clean_polars
from processor.load_data import load_pandas, load_polars
from main import app

def test_load_pandas():
    """
    Check if the data is loaded as a pandas DataFrame
    """
    df = load_pandas
    assert isinstance(df, pd.DataFrame)

def test_load_polars():
    """
    Check if the data is loaded as a polars DataFrame
    """
    df = load_polars
    assert isinstance(df, pl.DataFrame)

def test_clean_pandas():
    """
    Check if the data is cleaned as a pandas DataFrame
    """
    df = pd.DataFrame({"A": [1, None, 3], "B": [4, 5, None]})
    cleaned_df = clean_pandas(df)

    # check for missing values
    assert cleaned_df.isnull().sum().sum() == 0

def test_clean_polars():
    """
    Check if the data is cleaned as a polars DataFrame
    """
    df = pl.DataFrame({"A": [1, None, 3], "B": [4, 5, None]})
    cleaned_df = clean_polars(df)

    # check for missing values
    assert cleaned_df.null_count().sum() == 0

def test_aggregate_pandas():
    """
    Check if the data is aggregated as a pandas DataFrame
    """
    df = pd.DataFrame({"Names": ["John", "John", "Thanos"], "Scores": [4, 5, 6]})
    aggregated_df = aggregate_pandas(df)

    # check for the sum of the Scores column
    assert "sum" in aggregated_df.columns
    assert aggregated_df.loc[aggregated_df["Name"] == "John", "sum"].values[0] == 9

def test_aggregate_polars():
    """
    Check if the data is aggregated as a polars DataFrame
    """
    df = pl.DataFrame({"Names": ["John", "John", "Thanos"], "Scores": [4, 5, 6]})
    aggregated_df = aggregate_polars(df)

    # check for the sum of the Scores column
    assert "sum" in aggregated_df.columns
    assert aggregated_df.filter(pl.col("Name") == "John")["sum"][0] == 9

def test_process_data():
    """
    Check if the data is processed correctly
    """
    response = app.get("/process-data")
    assert response.status_code == 200
    assert response.json() is not None

def test_download_json():
    """
    Check if the JSON file is downloaded
    """
    response = app.get("/download-json")
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"

def test_download_parquet():
    """
    Check if the Parquet file is downloaded
    """
    response = app.get("/download-parquet")
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/octet-stream"
