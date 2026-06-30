from fileinput import filename

import requests
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import udf, col, to_date
import pyspark.sql.functions as F
import time

def download_csv(url: str, filename: str) -> str:

    print("Downloading", filename)
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
    
    owners = download_csv(url, "business_owners.csv")
    licenses = download_csv(url1, "business_licenses.csv")

    spark = SparkSession.builder.appName("Chicago Data").getOrCreate()

    # Read Csvs
    business_owners = spark.read.csv(owners, header=True, inferSchema=True)
    business_licenses = spark.read.csv(licenses, header=True, inferSchema=True)

    # Cache Csvs in memory
    business_owners.cache()
    business_owners.count()
    business_licenses.cache()
    business_licenses.count()

    # Delete Csvs from local Computer
    os.remove(owners)    
    os.remove(licenses)

    # Copy DataFrames
    clean_business_owners = business_owners
    clean_business_licenses = business_licenses

    # Clean Dataframes
    clean_business_owners.withColumn("Account Number", F.trim(col("Account Number").cast(StringType())))

    columns = ["Owner Last Name", "Owner First Name", "Owner Middle Initial"]
    for colm in columns:
        clean_business_owners.withColumn(colm, F.trim(F.initcap(colm)))
    
    clean_business_owners.withColumn("full_name", F.concat(col(columns[0]), F.lit(","), col(columns[1]), F.lit(" "), col(columns[2])).cast(StringType()))
    clean_business_owners.dropDuplicates()
    

    clean_business_licenses.withColumn("Account Number", col("Account Number").cast(StringType()))
    clean_business_licenses.withColumn("APPLICATION CREATED DATE", to_date(col("APPLICATION CREATED DATE"), "yyyy-MM-dd"))


if __name__ == "__main__":
    main()