import requests
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession

def fetch_business_owner_data() -> list:
    """
    Fetches business owner data from the Chicago Data Portal API.

    Returns:
        list: A list of dictionaries containing business owner data.
    """
    url = os.getenv("BUSINESSOWNERSURL")

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
    url = os.getenv("BUSINESSLICENSEURL")
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
    business_owner_data = fetch_business_owner_data()
    business_license_data = fetch_business_license_data()
    
    spark = SparkSession.builder.appName("Chicago Data").getOrCreate()

    df = spark.createDataFrame(business_license_data)
    df.show()   

if __name__ == "__main__":
    main()

