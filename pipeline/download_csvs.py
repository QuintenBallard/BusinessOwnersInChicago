import requests
import time
import os
from dotenv import load_dotenv

PROJECT_ROOT = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "..",
    )
)

ENV_FILE = os.path.join(
    PROJECT_ROOT,
    ".env",
)

OWNERS_CSV = os.path.join(
    PROJECT_ROOT,
    "business_owners.csv",
)

LICENSES_CSV = os.path.join(
    PROJECT_ROOT,
    "business_licenses.csv",
)

def download_csv(url: str, filename: str, retries: int = 3) -> str:

    temp_file = filename + ".part"

    for attempt in range(1, retries + 1):
        try:
            print(f"Downloading {filename} — attempt {attempt}/{retries}")

            with requests.get(
                url,
                stream=True,
                timeout=(15, 600)
            ) as response:
                response.raise_for_status()

                with open(temp_file, "wb") as file:
                    for chunk in response.iter_content(
                        chunk_size=8 * 1024 * 1024
                    ):
                        if chunk:
                            file.write(chunk)

            os.replace(temp_file, filename)

            print(f"Downloaded {filename} successfully.")
            return filename

        except (
            requests.exceptions.RequestException,
            OSError,
        ) as error:
            print(f"Download failed: {error}")

            if os.path.exists(temp_file):
                os.remove(temp_file)

            if attempt < retries:
                print("Retrying in 5 seconds...")
                time.sleep(5)
            else:
                raise RuntimeError(
                    f"Could not download {filename} "
                    f"after {retries} attempts."
                ) from error

def download_business_owners() -> str:
    load_dotenv()

    url = os.getenv("BUSINESSOWNERSCSVURL")

    if not url:
        raise RuntimeError(
            "BUSINESSOWNERSCSVURL is missing from .env."
        )

    return download_csv(
        url=url,
        filename=OWNERS_CSV,
    )


def download_business_licenses() -> str:
    load_dotenv()

    url = os.getenv("BUSINESSLICENSECSVURL")

    if not url:
        raise RuntimeError(
            "BUSINESSLICENSECSVURL is missing from .env."
        )

    return download_csv(
        url=url,
        filename=LICENSES_CSV,
    )