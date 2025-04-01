import sys
import os
from pathlib import Path
import streamlit as st
import pandas as pd
import plotly.express as px
from pymongo import MongoClient

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

# Now import your config
from config.settings import Config

# Initialize MongoDB connection
@st.cache_resource
def init_mongo():
    client = MongoClient(Config.MONGO_URI)
    return client[Config.DB_NAME][Config.PRODUCTS_COLLECTION]

def get_filtered_products(collection, filters):
    query = {}
    
    if filters['price_range']:
        query['price'] = {
            '$gte': filters['price_range'][0],
            '$lte': filters['price_range'][1]
        }
    
    if filters['sources']:
        query['source'] = {'$in': filters['sources']}
    
    if filters['min_rating'] > 0:
        query['rating'] = {'$gte': filters['min_rating']}
    
    projection = {
        '_id': 0,
        'name': 1,
        'price': 1,
        'rating': 1,
        'source': 1,
        'price_category': 1,
        'specifications': 1
    }
    
    products = list(collection.find(query, projection))
    return pd.DataFrame(products)

def main():
    st.set_page_config(page_title="E-Commerce Analytics", layout="wide")
    st.title("🛍️ E-Commerce Product Dashboard")
    
    try:
        collection = init_mongo()
        
        st.sidebar.header("Filters")
        sources = collection.distinct("source")
        selected_sources = st.sidebar.multiselect(
            "Select Sources", 
            options=sources,
            default=sources
        )
        
        price_range = st.sidebar.slider(
            "Price Range (₹)",
            min_value=0,
            max_value=100000,
            value=(0, 50000),
            step=1000
        )
        
        min_rating = st.sidebar.slider(
            "Minimum Rating",
            min_value=0.0,
            max_value=5.0,
            value=0.0,
            step=0.5
        )
        
        filters = {
            'sources': selected_sources,
            'price_range': price_range,
            'min_rating': min_rating
        }
        
        df = get_filtered_products(collection, filters)
        
        if df.empty:
            st.warning("No products found matching your filters!")
            return
            
        st.header("Overview Metrics")
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Products", len(df))
        col2.metric("Average Price", f"₹{df['price'].mean():,.2f}")
        col3.metric("Average Rating", f"{df['rating'].mean():.1f} ⭐")
        
        st.header("Price Distribution")
        fig1 = px.histogram(
            df, 
            x='price', 
            color='source',
            nbins=20,
            labels={'price': 'Price (₹)'}
        )
        st.plotly_chart(fig1, use_container_width=True)
        
        st.header("Price vs Rating")
        fig2 = px.scatter(
            df,
            x='price',
            y='rating',
            color='source',
            hover_data=['name'],
            size='rating'
        )
        st.plotly_chart(fig2, use_container_width=True)
        
        st.header("Products by Price Category")
        fig3 = px.pie(
            df,
            names='price_category',
            title="Product Distribution by Price Category"
        )
        st.plotly_chart(fig3, use_container_width=True)
        
        st.header("Product Data")
        st.dataframe(df)
        
    except Exception as e:
        st.error(f"An error occurred: {str(e)}")
        st.stop()

if __name__ == "__main__":
    main()