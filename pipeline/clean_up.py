import os

def delete_downloaded_csvs(owners_csv: str, licenses_csv: str) -> dict[str, object]:
    """
    Delete the downloaded CSV files and any incomplete
    temporary download files.

    This function should only be called after the Parquet
    validation task succeeds.

    Args:
        owners_csv:
            Path to the downloaded business owners CSV.

        licenses_csv:
            Path to the downloaded business licenses CSV.

    Returns:
        A small cleanup summary that Airflow can store.
    """
    csv_files = [
        owners_csv,
        licenses_csv,
    ]

    deleted_files = []

    for csv_file in csv_files:
        if os.path.exists(csv_file):
            os.remove(csv_file)
            deleted_files.append(csv_file)
            print(f"Deleted CSV: {csv_file}")
        else:
            print(f"CSV does not exist: {csv_file}")

        partial_file = csv_file + ".part"

        if os.path.exists(partial_file):
            os.remove(partial_file)
            deleted_files.append(partial_file)
            print(f"Deleted partial file: {partial_file}")

    print("CSV cleanup completed.")

    return {
        "cleanup_completed": True,
        "deleted_files": deleted_files,
    }