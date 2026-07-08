import os
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType
import pyspark.sql.functions as F

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
    
    df = df.withColumn("Account Number", F.trim(F.col("Account Number").cast(StringType())))

    columns = ["Owner Last Name", "Owner First Name", "Owner Middle Initial"]
    for colm in columns:
        df = df.withColumn(colm, F.trim(F.initcap(colm)))
    
    df = df.withColumn("full_name",F.concat_ws(" ", F.concat_ws(", ", F.col("Owner Last Name"), F.col("Owner First Name")), F.col("Owner Middle Initial")))
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

    df = df.filter(F.col("account_number").isNotNull() & F.col("license_id").isNotNull())

    df = df.dropDuplicates()

    return df

def merge_dfs(left: DataFrame, right: DataFrame) -> DataFrame:
    left = left.alias("left")
    right = right.alias("right")

    join_column = "account_number"

    intersection = [
        column
        for column in left.columns
        if column in right.columns
    ]

    left_columns = [
        column
        for column in left.columns
        if column not in right.columns
    ]

    right_columns = [
        column
        for column in right.columns
        if column not in left.columns
    ]

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

PROJECT_ROOT = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "..",
    )
)

PARQUET_DIRECTORY = os.path.join(
    PROJECT_ROOT,
    "parquet",
)

PARQUET_OUTPUTS = {
    "original_business_owners": os.path.join(
        PARQUET_DIRECTORY,
        "original_business_owners",
    ),
    "original_business_licenses": os.path.join(
        PARQUET_DIRECTORY,
        "original_business_licenses",
    ),
    "business_owners": os.path.join(
        PARQUET_DIRECTORY,
        "business_owners",
    ),
    "business_licenses": os.path.join(
        PARQUET_DIRECTORY,
        "business_licenses",
    ),
    "business_license_owners": os.path.join(
        PARQUET_DIRECTORY,
        "business_license_owners",
    ),
}


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("BusinessOwnersInChicago")
        .master("local[4]")
        .config("spark.sql.shuffle.partitions", "8")
        .config(
            "spark.ui.showConsoleProgress",
            "false",
        )
        .getOrCreate()
    )


def write_parquet(df: DataFrame, output_path: str) -> None:
    os.makedirs(
        os.path.dirname(output_path),
        exist_ok=True,
    )

    (
        df.write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(output_path)
    )


def transform_and_write_parquet(owners_csv: str, licenses_csv: str) -> dict[str, str]:
    spark = create_spark_session()

    try:
        business_owners = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(owners_csv)
        )

        business_licenses = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .option("mode", "FAILFAST")
            .option("quote", '"')
            .option("escape", '"')
            .option("multiLine", True)
            .csv(licenses_csv)
        )

        clean_business_owners = clean_owner_df(business_owners)

        clean_business_licenses = clean_license_df(business_licenses)

        merged_df = merge_dfs(clean_business_licenses, clean_business_owners)

        write_parquet(business_owners, PARQUET_OUTPUTS["original_business_owners"])

        write_parquet(business_licenses, PARQUET_OUTPUTS["original_business_licenses"])

        write_parquet(clean_business_owners, PARQUET_OUTPUTS["business_owners"])

        write_parquet(clean_business_licenses, PARQUET_OUTPUTS["business_licenses"])

        write_parquet(merged_df, PARQUET_OUTPUTS["business_license_owners"])

        print("Transformation and Parquet writing completed.")

        return PARQUET_OUTPUTS

    finally:
        spark.stop()
        print("Transformation Spark session stopped.")