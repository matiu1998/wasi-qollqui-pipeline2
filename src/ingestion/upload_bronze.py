# src/ingestion/upload_bronze.py

import os
import tempfile
import pandas as pd

from src.config.settings import RAW_DATA_PATH, BRONZE_PATH
from src.utils.gcs_client import upload_file
from src.utils.logger import get_logger

logger = get_logger(__name__)


def process_csv_file(csv_path: str):
    """
    Read CSV, convert to Parquet, and upload to GCS Bronze layer.
    """

    try:

        file_name = os.path.basename(csv_path)
        table_name = file_name.replace(".csv", "")

        logger.info(f"Processing file: {file_name}")

        # Leer CSV
        df = pd.read_csv(csv_path)

        if df.empty:
            logger.warning(f"{file_name} is empty. Skipping.")
            return

        # Crear archivo temporal parquet
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            parquet_path = tmp.name

        # Convertir a parquet
        df.to_parquet(parquet_path, engine="pyarrow", index=False)

        # Ruta en GCS
        gcs_path = f"{BRONZE_PATH}/{table_name}/{table_name}.parquet"

        # Subir archivo
        upload_file(parquet_path, gcs_path)

        logger.info(f"Uploaded to Bronze: {gcs_path}")

        # Limpiar temporal
        os.remove(parquet_path)

    except Exception as e:

        logger.error(f"Error processing file {csv_path}: {str(e)}")
        raise


def run_ingestion():
    """
    Process all CSV files in raw data folder.
    """

    logger.info("Starting Bronze ingestion pipeline")

    if not os.path.exists(RAW_DATA_PATH):
        logger.error(f"Raw data path not found: {RAW_DATA_PATH}")
        return

    for file in os.listdir(RAW_DATA_PATH):

        if file.endswith(".csv"):

            csv_path = os.path.join(RAW_DATA_PATH, file)

            process_csv_file(csv_path)

    logger.info("Bronze ingestion completed")


if __name__ == "__main__":
    run_ingestion()