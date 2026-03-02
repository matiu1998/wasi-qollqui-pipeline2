# src/processing/bronze_to_silver.py

import os
from src.utils.logger import get_logger
from src.config.settings import BRONZE_PATH, SILVER_PATH

logger = get_logger(__name__)


def transform():
    """
    Transform data from Bronze layer to Silver layer.
    """

    logger.info("Starting Bronze → Silver transformation")

    try:

        # Aquí en el futuro leeremos datos desde Bronze
        logger.info(f"Reading data from: {BRONZE_PATH}")

        # Aquí irán transformaciones
        logger.info("Applying data cleaning and transformations")

        # Aquí guardaremos datos en Silver
        logger.info(f"Writing transformed data to: {SILVER_PATH}")

        logger.info("Bronze → Silver transformation completed")

    except Exception as e:

        logger.error(f"Error in Bronze → Silver transformation: {str(e)}")

        raise


if __name__ == "__main__":
    transform()