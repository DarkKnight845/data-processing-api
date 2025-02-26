# **Data Processing API**
## 📌 Project Overview
This project is a FastAPI-based web service designed to process a dataset using **Polars** and **Pandas**. It provides endpoints for:
1. Loading, cleaning and aggregating data
2. Compares the performance of Polars and Pandas
3. Saves processed data in JSON and Parquet formats.
4. Exposing RESTful API endpoints for retrieving processed data.
The project uses  - [the Online Retail II dataset](https://archive.ics.uci.edu/ml/machine-learning-databases/00502/online_retail_II.xlsx)

## 🚀 Features
✅ Data Loading: Supports both Pandas and Poalrs for data ingestion.    
✅ Data Aggregation: Groups data by a categorical column and computes **sum, mean** and **count**.  
✅ API Endpoints: Provides RESTful endpoints to fetch processed results.    
✅ Compares data loading and aggregation time for Paandas and Polars.   
✅ File Exporting: Saves cleaned and aggregated data as **JSON** and **Parquet** files. 
✅ Testing: Implements pytest for testing API and data processing functions.

## 🛠️ Installation Requirements
1. **Clone the Repository**
```
git remote add origin https://github.com/DarkKnight845/data-processing-api.git
```
2. **Setup Virtual Environment and Install Dependencies**
Ensure **Poetry** is installed
```
pip install poetry
```
Then, install dependencies:
```
poetry instal package
```
▶️ **Running the Application**
Start the FastAPI serveer with poetry:
```
poetry run uvicorn main:app --reload
```
⚡ **API Endpoints**
| Endpoint       | Method | Description                           |
|----------------|--------|---------------------------------------|
| `/process-data`    | GET   | Loads, cleans and aggregates data                 |
| `/download-json`| GET   | Downloads processed data in JSON format            |
| `/download-parquet`      | GET    | Downloads processed data in Parquet format           |
| `/aggregate` | GET    | Expose aggregaated results in Polars and Pandas |

🧪 **Running Tests**
```
poetry run pytest
```