import polars as pl
import pandas as pd

data_url = "https://archive.ics.uci.edu/ml/machine-learning-databases/00502/online_retail_II.xlsx"

def aggregate_pandas(data1: pd.DataFrame):
    """
    Aggregates the data using pandas to get the total sales per country
    Also: Compute the mean and amount of Quantity per country
    """
    try:
        return data1.groupby("Country")["Quantity"].agg(["sum", "mean", "count"])
    except Exception as e:
        print(f'Error while aggregating Pandas DataFrame: {e}')
        return data1

data_pd = pd.read_excel(data_url, sheet_name="Year 2009-2010")
print(aggregate_pandas(data_pd))

def aggregate_polars(data2: pl.DataFrame):
    """
    Aggregates the data using polars to get the total sales per country
    Also: Compute the mean and amount of Quantity per country
    """
    try:
        return data2.group_by("Country").agg([pl.col("Quantity").sum().alias("sum"), pl.col("Quantity").mean().alias("mean"), pl.col("Quantity").count().alias("count")])
    except Exception as e:
        print(f'Error while aggregating Polars DataFrame: {e}')
        return data2






"""
Saving Processed Data to a JSON & Parquet File
"""


def save_to_json_pd(df: pd.DataFrame, file_name: str):
    """
    Save the DataFrame to a JSON file
    """
    try:
        df.to_json(file_name, orient="records")
        return f"Data saved to {file_name}"
    except Exception as e:
        return f"Error while saving data: {e}"

def save_to_json_pl(df: pl.DataFrame, file_name: str):
    """
    Save the DataFrame to a JSON file
    """
    try:
        df.write_json(file_name)
        return f"Data saved to {file_name}"
    except Exception as e:
        return f"Error while saving data: {e}"
    
def save_to_parquet_pd(df: pd.DataFrame, file_name: str):
    """
    Save the DataFrame to a Parquet file
    """
    try:
        df.to_parquet(file_name, engine="pyarrow", compression="snappy")
        return f"Data saved to {file_name}"
    except Exception as e:
        return f"Error while saving data: {e}"

def save_to_parquet_pl(df: pl.DataFrame, file_name: str):
    """
    Save the DataFrame to a Parquet file
    """
    try:
        df.write_parquet(file_name)
        return f"Data saved to {file_name}"
    except Exception as e:
        return f"Error while saving data: {e}"




data_pd = pd.read_excel(data_url, sheet_name="Year 2009-2010")
data_pl = pl.read_excel(data_url, sheet_name="Year 2010-2011")
    
print(f"Pand Json File saved: {save_to_json_pd(aggregate_pandas(data_pd), "pandas_data.json")}")
print(f"Polars Json File saved: {save_to_json_pl(aggregate_polars(data_pl), "polars_data.json")}")
print(f"Pandas Parquet File saved: {save_to_parquet_pd(aggregate_pandas(data_pd), "pandas_data.parquet")}")
print(f"Polars Parquet File saved: {save_to_parquet_pl(aggregate_polars(data_pl), "polars_data.parquet")}") 
# Pand Json File saved: Data saved to pandas_data.json
