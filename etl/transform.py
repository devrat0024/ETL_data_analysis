import pandas as pd
import numpy as np
from datetime import datetime

def clean_product_data(products):
    """Clean and normalize scraped product data"""
    if not products:
        return pd.DataFrame()
    
    df = pd.DataFrame(products)
    
    # Handle missing values
    df['name'] = df['name'].fillna('Unknown Product').str.strip()
    df['source'] = df['source'].fillna('Unknown')
    
    # Convert numeric fields
    df['price'] = pd.to_numeric(df['price'], errors='coerce').fillna(0)
    df['rating'] = pd.to_numeric(df['rating'], errors='coerce').clip(0, 5).fillna(0)
    
    # Add metadata
    df['scraped_at'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    
    # Create price categories
    bins = [0, 1000, 5000, 10000, 20000, 50000, np.inf]
    labels = ['0-1K', '1K-5K', '5K-10K', '10K-20K', '20K-50K', '50K+']
    df['price_category'] = pd.cut(df['price'], bins=bins, labels=labels)
    
    return df

def prepare_for_mongodb(df):
    """Convert DataFrame to MongoDB-compatible format"""
    if df.empty:
        return []
    
    # Convert to records and handle NaN values
    records = df.replace({np.nan: None}).to_dict('records')
    
    # Ensure all records have required fields
    for record in records:
        record.setdefault('specifications', {})
        record.setdefault('price_category', 'Unknown')
    
    return records