import os
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file

class Config:
    """Central configuration for the ETL pipeline"""
    
    # MongoDB Configuration
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    DB_NAME = "ecommerce_db"
    PRODUCTS_COLLECTION = "products"
    
    # Scraping Settings
    FLIPKART_BASE_URL = "https://www.flipkart.com"
    REQUEST_DELAY = (2, 5)  # Random delay between requests (min, max seconds)
    MAX_RETRIES = 3
    USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36..."
    
    # HTML Class Selectors (Update these if Flipkart changes their HTML)
    PRODUCT_SELECTORS = {
        'name': '_4rR01T',
        'price': '_30jeq3',
        'rating': '_3LWZlK',
        'specs_container': '_3k-BhJ'
    }