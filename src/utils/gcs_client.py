# src/utils/gcs_client.py

from google.cloud import storage
from src.config.settings import BUCKET_NAME
from src.utils.logger import get_logger

logger = get_logger(__name__)


def get_client():
    """
    Create and return a Google Cloud Storage client
    """
    return storage.Client()


def upload_file(local_path: str, gcs_path: str):
    """
    Upload a local file to Google Cloud Storage

    Args:
        local_path (str): path of local file
        gcs_path (str): destination path in GCS bucket
    """

    try:

        client = get_client()

        bucket = client.bucket(BUCKET_NAME)

        blob = bucket.blob(gcs_path)

        blob.upload_from_filename(local_path)

        logger.info(f"Uploaded {local_path} → gs://{BUCKET_NAME}/{gcs_path}")

    except Exception as e:

        logger.error(f"Error uploading file {local_path}: {str(e)}")

        raise