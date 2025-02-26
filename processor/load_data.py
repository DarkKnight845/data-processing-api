import pandas as pd
import polars as pl
import time

data_url = "https://archive.ics.uci.edu/ml/machine-learning-databases/00502/online_retail_II.xlsx"
# print(pd.read_excel(data_url, sheet_name="Year 2009-2010").head())

def load_pandas():
    start_time = time.time()
    df = pd.read_excel(data_url, sheet_name="Year 2009-2010")
    # print(f"Pandas loading time: {time.time() - start_time}")

    # Aggregating data to get the total sales per country
    agg_start_time = time.time()
    df.groupby("Country")["Quantity"].sum()
    # print(f"Pandas aggregation time: {time.time() - agg_start_time}")

    return f"Pandas loading time: {time.time() - start_time} | Pandas aggregation time: {time.time() - agg_start_time}" 

def load_polars():
    start_time = time.time()
    df = pl.read_excel(data_url, sheet_name="Year 2009-2010")
    # print(f"Polars loading time: {time.time() - start_time}")
    
    # Aggregating data to get the total sales per country
    agg_start_time = time.time()
    df.group_by("Country").agg(pl.col("Quantity").sum())
    # print(f"Polars aggregation time: {time.time() - agg_start_time}")

    return f"Polars loading time: {time.time() - start_time} | Polars aggregation time: {time.time() - agg_start_time}"

def benchmark():
    print(load_pandas())
    # print(load_polars())
    return {"pandas": load_pandas(), "polars": load_polars()}

if __name__ == "__main__":
    print("Benchmark complete:", benchmark())