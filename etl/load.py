from pymongo import MongoClient, ASCENDING
from pymongo.errors import BulkWriteError
from config.settings import Config
import logging

class MongoDBLoader:
    """Handles all MongoDB operations"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.client = MongoClient(
            Config.MONGO_URI,
            connectTimeoutMS=5000,
            serverSelectionTimeoutMS=5000
        )
        self.db = self.client[Config.DB_NAME]
        self.collection = self.db[Config.PRODUCTS_COLLECTION]
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()
    
    def ensure_indexes(self):
        """Create required indexes for performance"""
        try:
            # Unique compound index
            self.collection.create_index(
                [('name', ASCENDING), ('source', ASCENDING)],
                unique=True,
                name='product_identity'
            )
            # Performance indexes
            self.collection.create_index([('price', ASCENDING)])
            self.collection.create_index([('rating', ASCENDING)])
        except Exception as e:
            self.logger.error(f"Index creation failed: {str(e)}")
    
    def insert_products(self, products):
        """Bulk insert products with update-on-duplicate"""
        if not products:
            return 0
        
        operations = [{
            'updateOne': {
                'filter': {'name': p['name'], 'source': p['source']},
                'update': {'$set': p},
                'upsert': True
            }
        } for p in products]
        
        try:
            result = self.collection.bulk_write(operations, ordered=False)
            return result.upserted_count + result.modified_count
        except BulkWriteError as bwe:
            self.logger.warning(f"Completed with {len(bwe.details['writeErrors'])} errors")
            return len(operations) - len(bwe.details['writeErrors'])