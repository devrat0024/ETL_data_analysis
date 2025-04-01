import requests
from bs4 import BeautifulSoup
import random
import time
from urllib.parse import urljoin
from config.settings import Config

class FlipkartScraper:
    """Handles all Flipkart scraping operations"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': Config.USER_AGENT})
        
    def _request_with_retry(self, url, max_retries=Config.MAX_RETRIES):
        """Make HTTP request with retry logic"""
        for attempt in range(max_retries):
            try:
                time.sleep(random.uniform(*Config.REQUEST_DELAY))
                response = self.session.get(url, timeout=10)
                response.raise_for_status()
                return response
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                time.sleep(5 * (attempt + 1))
    
    def scrape_product_page(self, product_url):
        """Scrape detailed product information from a product page"""
        try:
            response = self._request_with_retry(product_url)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            return {
                'name': self._extract_text(soup, Config.PRODUCT_SELECTORS['name']),
                'price': self._extract_price(soup),
                'rating': self._extract_rating(soup),
                'specifications': self._extract_specs(soup),
                'url': product_url,
                'source': 'Flipkart',
                'scraped_at': time.strftime("%Y-%m-%d %H:%M:%S")
            }
        except Exception as e:
            print(f"Failed to scrape {product_url}: {str(e)}")
            return None
    
    def _extract_text(self, soup, class_name):
        """Extract text from HTML element"""
        element = soup.find(class_=class_name)
        return element.get_text(strip=True) if element else None
    
    def _extract_price(self, soup):
        """Extract and parse price"""
        price_text = self._extract_text(soup, Config.PRODUCT_SELECTORS['price'])
        return float(price_text.replace('₹', '').replace(',', '')) if price_text else 0.0
    
    def _extract_rating(self, soup):
        """Extract and parse rating"""
        rating_text = self._extract_text(soup, Config.PRODUCT_SELECTORS['rating'])
        try:
            return float(rating_text) if rating_text else 0.0
        except ValueError:
            return 0.0
    
    def _extract_specs(self, soup):
        """Extract product specifications"""
        specs = {}
        containers = soup.find_all(class_=Config.PRODUCT_SELECTORS['specs_container'])
        for container in containers:
            key = self._extract_text(container, '_1hKmbr')
            value = self._extract_text(container, '_21lJbe')
            if key and value:
                specs[key] = value
        return specs

def scrape_flipkart_search(query, max_products=10):
    """Search Flipkart and scrape product listings"""
    scraper = FlipkartScraper()
    base_url = f"{Config.FLIPKART_BASE_URL}/search"
    params = {'q': query}
    products = []
    
    try:
        response = scraper._request_with_retry(f"{base_url}?{'&'.join(f'{k}={v}' for k,v in params.items())}")
        soup = BeautifulSoup(response.text, 'html.parser')
        
        product_links = [
            urljoin(Config.FLIPKART_BASE_URL, a['href'])
            for a in soup.select('a[href*="/p/"]')[:max_products]
            if '/p/' in a.get('href', '')
        ]
        
        for link in product_links:
            product = scraper.scrape_product_page(link)
            if product:
                products.append(product)
                if len(products) >= max_products:
                    break
                    
    except Exception as e:
        print(f"Search failed: {str(e)}")
    
    return products