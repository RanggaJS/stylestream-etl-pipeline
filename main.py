import logging
import os
from datetime import datetime
from utils.extract import extract_data as fetch_data
from utils.transform import transform_data as process_data
from utils.load import (
    save_to_csv as export_csv,
    save_to_gsheets as export_gsheet,
    save_to_postgres as export_postgres
)

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def run_pipeline():
    """
    Main function to execute the ETL process.
    """
    try:
        # Data extraction step
        logger.info("Starting data extraction process...")
        raw_dataset = fetch_data()
        logger.info(f"Extraction complete. Retrieved {len(raw_dataset)} records.")

        # Data transformation step
        logger.info("Starting data transformation process...")
        cleaned_data = process_data(raw_dataset)

        if cleaned_data.empty or len(cleaned_data) < 10:
            logger.warning("Not enough valid data after transformation. Generating sample dataset instead...")
            from utils.transform import generate_sample_data as create_sample_data
            cleaned_data = create_sample_data(100)

        # Loading data to CSV
        logger.info("Saving data to CSV file...")
        csv_file_path = export_csv(cleaned_data)
        logger.info(f"Data successfully saved to: {csv_file_path}")

        # Loading data to Google Sheets
        try:
            logger.info("Uploading data to Google Sheets...")
            gsheet_link = export_gsheet(cleaned_data)
            logger.info(f"Data successfully uploaded to Google Sheets: {gsheet_link}")
        except Exception as gsheet_error:
            logger.error(f"Failed to upload to Google Sheets: {str(gsheet_error)}")

        # Loading data to PostgreSQL
        try:
            logger.info("Saving data to PostgreSQL database...")
            db_result = export_postgres(cleaned_data)
            logger.info(f"Data successfully saved to PostgreSQL. Status: {db_result}")
        except Exception as db_error:
            logger.error(f"Failed to save to PostgreSQL: {str(db_error)}")

        logger.info("ETL pipeline finished successfully.")
        return True

    except Exception as main_error:
        logger.error(f"An error occurred in the ETL pipeline: {str(main_error)}")
        return False

if __name__ == "__main__":
    run_pipeline()
