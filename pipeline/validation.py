import os
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pipeline.transform import create_spark_session, PARQUET_OUTPUTS

REQUIRED_COLUMNS = {
    "business_owners": [
        "account_number",
        "full_name",
    ],
    "business_licenses": [
        "account_number",
        "license_id",
    ],
    "business_license_owners": [
        "account_number",
        "license_id",
    ],
}


def validate_folder_exists(
    dataset_name: str,
    parquet_path: str,
) -> None:
    """
    Confirm that a Parquet output directory exists.

    Args:
        dataset_name:
            The name used to identify the dataset.

        parquet_path:
            The location of the Parquet directory.

    Raises:
        FileNotFoundError:
            If the expected Parquet directory does not exist.
    """
    if not os.path.exists(parquet_path):
        raise FileNotFoundError(
            f"Missing Parquet output for "
            f"{dataset_name}: {parquet_path}"
        )

    if not os.path.isdir(parquet_path):
        raise ValueError(
            f"The Parquet output path for {dataset_name} "
            f"is not a directory: {parquet_path}"
        )


def validate_dataframe_not_empty(
    dataset_name: str,
    df: DataFrame,
) -> None:
    """
    Confirm that a DataFrame contains at least one row.

    Args:
        dataset_name:
            The name used to identify the dataset.

        df:
            The Spark DataFrame being validated.

    Raises:
        ValueError:
            If the DataFrame contains no rows.
    """
    has_rows = df.limit(1).count() > 0

    if not has_rows:
        raise ValueError(
            f"{dataset_name} is empty."
        )


def validate_required_columns(
    dataset_name: str,
    df: DataFrame,
) -> None:
    """
    Confirm that all required columns exist.

    Args:
        dataset_name:
            The name used to identify the dataset.

        df:
            The Spark DataFrame being validated.

    Raises:
        ValueError:
            If one or more required columns are missing.
    """
    required_columns = REQUIRED_COLUMNS.get(
        dataset_name,
        [],
    )

    missing_columns = [
        column_name
        for column_name in required_columns
        if column_name not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"{dataset_name} is missing required "
            f"columns: {missing_columns}"
        )


def validate_no_null_values(
    dataset_name: str,
    df: DataFrame,
    column_names: list[str],
) -> None:
    """
    Confirm that important columns do not contain null values.

    Args:
        dataset_name:
            The name used to identify the dataset.

        df:
            The Spark DataFrame being validated.

        column_names:
            Columns that should not contain null values.

    Raises:
        ValueError:
            If an expected column is missing or contains null values.
    """
    for column_name in column_names:
        if column_name not in df.columns:
            raise ValueError(
                f"Cannot validate {column_name} in "
                f"{dataset_name} because the column is missing."
            )

        null_exists = (
            df
            .filter(
                F.col(column_name).isNull()
            )
            .limit(1)
            .count()
            > 0
        )

        if null_exists:
            raise ValueError(
                f"{dataset_name} contains null values "
                f"in {column_name}."
            )


def validate_cleaned_datasets(
    loaded_datasets: dict[str, DataFrame],
) -> None:
    """
    Apply validation rules specific to the cleaned and merged datasets.

    Args:
        loaded_datasets:
            A dictionary containing the Spark DataFrames that were
            successfully read from Parquet.
    """
    business_licenses = loaded_datasets[
        "business_licenses"
    ]

    merged_data = loaded_datasets[
        "business_license_owners"
    ]

    # clean_license_df() filters these fields, so the Parquet
    # output should not contain nulls in either column.
    validate_no_null_values(
        dataset_name="business_licenses",
        df=business_licenses,
        column_names=[
            "account_number",
            "license_id",
        ],
    )

    # The merged dataset uses cleaned licenses as the left side
    # of the join, so these fields should also remain non-null.
    validate_no_null_values(
        dataset_name="business_license_owners",
        df=merged_data,
        column_names=[
            "account_number",
            "license_id",
        ],
    )


def validate_parquet_outputs(
    parquet_paths: dict[str, str] | None = None,
) -> dict[str, object]:
    """
    Validate all Parquet outputs produced by the pipeline.

    This function checks that:

    1. Every expected Parquet folder exists.
    2. Spark can successfully read every dataset.
    3. Every dataset contains at least one row.
    4. Processed datasets contain their required columns.
    5. Important cleaned fields do not contain null values.

    Args:
        parquet_paths:
            Optional dictionary containing Parquet dataset names and
            their output paths. If not provided, the default project
            Parquet paths are used.

    Returns:
        dict:
            A small validation summary that Airflow can pass to the
            cleanup task through XCom.

    Raises:
        FileNotFoundError:
            If an expected output directory does not exist.

        ValueError:
            If a dataset is empty, missing columns, or contains
            invalid null values.

        Exception:
            If Spark cannot read a Parquet dataset.
    """
    if parquet_paths is None:
        parquet_paths = PARQUET_OUTPUTS

    expected_datasets = set(
        PARQUET_OUTPUTS.keys()
    )

    received_datasets = set(
        parquet_paths.keys()
    )

    missing_datasets = (
        expected_datasets - received_datasets
    )

    if missing_datasets:
        raise ValueError(
            "The validation task did not receive paths for: "
            f"{sorted(missing_datasets)}"
        )

    spark: SparkSession = create_spark_session()

    loaded_datasets: dict[str, DataFrame] = {}
    validation_results: dict[str, dict[str, object]] = {}

    try:
        for dataset_name, parquet_path in parquet_paths.items():
            print(
                f"Validating {dataset_name}..."
            )

            validate_folder_exists(
                dataset_name=dataset_name,
                parquet_path=parquet_path,
            )

            try:
                df = spark.read.parquet(
                    parquet_path
                )

            except Exception as error:
                raise RuntimeError(
                    f"Spark could not read the Parquet "
                    f"dataset {dataset_name}: {parquet_path}"
                ) from error

            validate_dataframe_not_empty(
                dataset_name=dataset_name,
                df=df,
            )

            validate_required_columns(
                dataset_name=dataset_name,
                df=df,
            )

            loaded_datasets[dataset_name] = df

            validation_results[dataset_name] = {
                "exists": True,
                "readable": True,
                "not_empty": True,
                "column_count": len(df.columns),
            }

            print(
                f"{dataset_name} passed basic validation."
            )

        validate_cleaned_datasets(
            loaded_datasets
        )

        print(
            "All Parquet outputs passed validation."
        )

        return {
            "validated": True,
            "validated_datasets": list(
                validation_results.keys()
            ),
            "results": validation_results,
        }

    finally:
        spark.stop()
        print(
            "Validation Spark session stopped."
        )