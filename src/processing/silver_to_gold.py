# src/processing/silver_to_gold.py

from src.utils.logger import get_logger
from src.config.settings import SILVER_PATH, GOLD_PATH

logger = get_logger(__name__)


def aggregate():
    """
    Aggregate data from Silver layer and generate Gold datasets (business KPIs).
    """

    logger.info("Starting Silver → Gold aggregation")

    try:

        logger.info(f"Reading data from Silver layer: {SILVER_PATH}")

        # Aquí se aplicarán agregaciones y métricas de negocio
        logger.info("Generating business KPIs")

        logger.info(f"Writing aggregated data to Gold layer: {GOLD_PATH}")

        logger.info("Silver → Gold aggregation completed")

    except Exception as e:

        logger.error(f"Error in Silver → Gold aggregation: {str(e)}")

        raise


if __name__ == "__main__":
    aggregate()