import requests
from pyspark.sql import SparkSession

url = "https://data.cityofchicago.org/resource/ezma-pppn.json"


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
    if len(data) < params["$limit"]:
        break
    params["$offset"] += params["$limit"]

spark = SparkSession.builder.appName("Chicago Data").getOrCreate()


df = spark.createDataFrame(results)
df.show()
