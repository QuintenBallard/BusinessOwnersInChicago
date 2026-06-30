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

def trim_all_cols(df: DataFrame) -> DataFrame:
    df = df.select([
        F.trim(F.col(c)).alias(c) if t == "string" else F.col(c) 
        for c, t in df.dtypes
    ])
    return df

def fix_col_names(df: DataFrame) -> DataFrame:
    df = df.toDF(*[
        column.strip().lower().replace(" ", "_")
        for column in df.columns
    ])
    return df

def clean_owner_df(df: DataFrame) -> DataFrame:
    
    df = df.withColumn("Account Number", F.trim(col("Account Number").cast(StringType())))

    columns = ["Owner Last Name", "Owner First Name", "Owner Middle Initial"]
    for colm in columns:
        df = df.withColumn(colm, F.trim(F.initcap(colm)))
    
    df = df.withColumn("full_name", F.concat(col(columns[0]), F.lit(","), col(columns[1]), F.lit(" "), col(columns[2])).cast(StringType()))
    df = trim_all_cols(df)
    df = fix_col_names(df)

    df = df.dropDuplicates()

    return df

def convert_col_to_date(df: DataFrame, lst: list[str]) -> DataFrame:
    for column in lst:
        if column in df.columns:
            df = df.withColumn(
                column,
                F.expr(
                    f"try_to_date(`{column}`, 'MM/dd/yyyy')"
                )
            )

    return df

def convert_col_to_number(df: DataFrame, lst: list[str], lst1: list[str]) -> DataFrame:
    for column_name in lst:
        if column_name in df.columns:
            df = df.withColumn(
                column_name,
                F.expr(f"try_cast(`{column_name}` AS INT)")
            )

    for column_name in lst1:
        if column_name in df.columns:
            df = df.withColumn(
                column_name,
                F.expr(f"try_cast(`{column_name}` AS DOUBLE)")
            )

    return df

def clean_license_df(df: DataFrame) -> DataFrame:
    
    date_columns = [
        "application_created_date",
        "application_requirements_complete",
        "payment_date",
        "license_term_start_date",
        "license_term_expiration_date",
        "license_approved_for_issuance",
        "date_issued",
        "license_status_change_date"
    ]

    
    
    integer_columns = [
        "license_id",
        "site_number",
        "ward",
        "precinct",
        "police_district",
        "community_area",
        "ssa"
    ]

    double_columns = [
        "latitude",
        "longitude"
    ]

    df = trim_all_cols(df)
    df = fix_col_names(df)

    df = convert_col_to_date(df, date_columns)
    df = convert_col_to_number(df, integer_columns, double_columns)

    df = df.filter(
        F.col("account_number").isNotNull()
        & F.col("license_id").isNotNull()
    )

    df = df.dropDuplicates()

    return df

def merge_dfs(left: DataFrame, right: DataFrame) -> DataFrame:
    left = left.alias("left")
    right = right.alias("right")

    join_column = "account_number"

    intersection = set(left.columns) & set(right.columns)
    left_columns = set(left.columns) - intersection
    right_columns = set(right.columns) - intersection

    merged_df = (
        left.join(
            right,
            on=join_column,
            how="left"
        )
        .select(
            F.col(join_column),
            *[
                F.coalesce(
                    F.col(f"left.`{column}`"),
                    F.col(f"right.`{column}`")
                ).alias(column)
                for column in intersection
                if column != join_column
            ],
            *[
                F.col(f"left.`{column}`").alias(column)
                for column in left_columns
            ],
            *[
                F.col(f"right.`{column}`").alias(column)
                for column in right_columns
            ]
        )
    )

    return merged_df

def main():
    load_dotenv()
    url = os.getenv("BUSINESSOWNERSCSVURL")
    url1 = os.getenv("BUSINESSLICENSECSVURL")
    
    owners = download_csv(url, "business_owners.csv")
    licenses = download_csv(url1, "business_licenses.csv")

    spark = SparkSession.builder.appName("Chicago Data").getOrCreate()

    # Read CSVs
    business_owners = spark.read.csv(owners, header=True, inferSchema=True)
    business_licenses = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .option("mode", "FAILFAST")
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", True)
        .csv(licenses)
    )

    # Cache Csvs in memory
    business_owners.cache()
    business_owners.count()
    business_licenses.cache()
    business_licenses.count()
    
    # Copy DataFrames
    clean_business_owners = business_owners
    clean_business_licenses = business_licenses
    
    # Clean Data
    clean_business_owners = clean_owner_df(clean_business_owners)
    clean_business_licenses = clean_license_df(clean_business_licenses)

    # Merge Datasets
    df = merge_dfs(clean_business_licenses, clean_business_owners)
    
    df.printSchema()
    df.show()

    # Delete CSVs from local Computer
    os.remove(owners)
    os.remove(licenses)

    spark.stop()

if __name__ == "__main__":
    main()