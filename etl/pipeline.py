import logging
from .extract import scrape_flipkart_search
from .transform import clean_product_data, prepare_for_mongodb
from .load import MongoDBLoader
from config.settings import Config

logger = logging.getLogger(__name__)

def run_etl_pipeline(search_query, max_products=10):
    """Execute the complete ETL process"""
    logger.info(f"Starting ETL for '{search_query}'")
    
    # Extraction
    try:
        logger.info(f"Scraping {max_products} products...")
        scraped_products = scrape_flipkart_search(search_query, max_products)
        if not scraped_products:
            logger.error("No products scraped")
            return False
        logger.info(f"Successfully scraped {len(scraped_products)} products")
    except Exception as e:
        logger.error(f"Scraping failed: {str(e)}")
        return False
    
    # Transformation
    try:
        logger.info("Cleaning and transforming data...")
        cleaned_df = clean_product_data(scraped_products)
        mongo_records = prepare_for_mongodb(cleaned_df)
        logger.info(f"Prepared {len(mongo_records)} records for MongoDB")
    except Exception as e:
        logger.error(f"Transformation failed: {str(e)}")
        return False
    
    # Loading
    try:
        logger.info("Loading data to MongoDB...")
        with MongoDBLoader() as loader:
            loader.ensure_indexes()
            inserted_count = loader.insert_products(mongo_records)
            logger.info(f"Successfully loaded {inserted_count} products")
        return True
    except Exception as e:
        logger.error(f"Loading failed: {str(e)}")
        return False