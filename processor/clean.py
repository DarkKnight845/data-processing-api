import pandas as pd
import polars as pl


data_url = "https://archive.ics.uci.edu/ml/machine-learning-databases/00502/online_retail_II.xlsx"


# data2 = pd.read_excel(data_url, sheet_name="Year 2009-2010")
# print(data.isnull().sum()) # check for missing values  --- there are missing values in the Descrp and Customer ID columns
# print(data.dtypes) # check data types 
# print(data.describe()) # check data distribution
# print(data.nunique()) # check unique values


def clean_pandas():
    """
    Cleans the data using pandas by filling missing values with mean
    """
    try:
        data1 = pd.read_excel(data_url, sheet_name="Year 2009-2010")
        for col in data1.columns:
            if data1[col].isnull().sum() > 0:
                data1[col].fillna(data1[col].mean(), inplace=True)
            else:
                pass
        return data1
    except Exception as e:
        print(f'Error while cleaning Pandas DataFrame: {e}')
        return data1    

def clean_polars():
    """
    Cleans the data using polars by filling missing values with mean
    """
    try:
        data2 = pl.read_excel(data_url, sheet_name="Year 2010-2011")
        for col in data2.columns:
            if data2[col].is_null().sum() > 0:
                data2[col].fill_null(data2[col].mean())
            else:
                pass
        return data2
    except Exception as e:
        print(f'Error while cleaning Polars DataFrame: {e}')
        return data2


print(clean_polars())


