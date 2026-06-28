from fileinput import filename

import requests
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, dat
from pyspark.sql.functions import udf, col, to_date
import time

def download_csv(url: str, filename: str) -> str:

    response = requests.get(url)
    if response.status_code == 200:
        with open(filename, "wb") as f:
            f.write(response.content)
        print(f"Downloaded {filename} successfully.")
    else:
        print(f"Failed to download {filename}. Status code: {response.status_code}")
    return filename

def fetch_business_owner_data() -> list:
    """
    Fetches business owner data from the Chicago Data Portal API.

    Returns:
        list: A list of dictionaries containing business owner data.
    """
    url = os.getenv("BUSINESSOWNERSAPIURL")

    params = {
        "$limit": 1000,
        "$offset": 0
    }

    results = []
    while True:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()
        results.extend(data)
        if len(data) < params["$limit"]:
            break
        params["$offset"] += params["$limit"]
    return results

def fetch_business_license_data() -> list:
    """
    Fetches business license data from the Chicago Data Portal API.

    Returns:
        list: A list of dictionaries containing business license data.
    """
    #url = os.getenv("BUSINESSLICENSEURL")
    url = os.getenv("BUSINESSLICENSEAPIURL")
    params = {
        "$limit": 1000,
        "$offset": 0
    }

    results = []
    while True:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()
        print(data)
        results.extend(data)
        if len(data) < params["$limit"] or len(data) == 0:
            break
        params["$offset"] += params["$limit"]
    return results

def main():
    load_dotenv()
    url = os.getenv("BUSINESSOWNERSCSVURL")
    url1 = os.getenv("BUSINESSLICENSECSVURL")
    
    filename = download_csv(url, "business_owners.csv")
    filename1 = download_csv(url1, "business_licenses.csv")

    spark = SparkSession.builder.appName("Chicago Data").getOrCreate()

    df = spark.read.csv(filename, header=True, inferSchema=True)
    df.cache()
    df.count()
    os.remove(filename)
    df.withColumn("account_number", col("account_number").cast(StringType()))

    df1 = spark.read.csv(filename1, header=True, inferSchema=True)
    df1.cache()
    df1.count()
    os.remove(filename1)
    df1.withColumn("account_number", col("account_number").cast(StringType()))
    df1.withColumn("application_created_date", to_date(col("application_created_date"), "yyyy-MM-dd"))

if __name__ == "__main__":
    main()