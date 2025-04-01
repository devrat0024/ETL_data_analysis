#!/usr/bin/env python3
import logging
import sys
from etl import run_etl_pipeline

def configure_logging():
    """Set up logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('etl.log'),
            logging.StreamHandler()
        ]
    )

def main():
    configure_logging()
    
    try:
        success = run_etl_pipeline(
            search_query="smartphones",
            max_products=15
        )
        sys.exit(0 if success else 1)
    except Exception as e:
        logging.critical(f"Pipeline failed: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()