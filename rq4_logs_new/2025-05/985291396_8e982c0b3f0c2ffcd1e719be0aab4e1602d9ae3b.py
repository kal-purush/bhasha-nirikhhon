import logging
import requests
from bs4 import BeautifulSoup
import trafilatura

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class SimpleCrawler:
    def __init__(self):
        """Initialize the SimpleCrawler"""
        self.pages = []  # List to store crawled pages
        
    def extract_text_content(self, url, html_content):
        """Extract clean text content from HTML"""
        try:
            # Use trafilatura for content extraction
            text = trafilatura.extract(html_content)
            
            # Fallback to BeautifulSoup if trafilatura returns None
            if text is None:
                soup = BeautifulSoup(html_content, 'html.parser')
                
                # Remove script and style elements
                for script in soup(["script", "style"]):
                    script.extract()
                
                # Get text
                text = soup.get_text(separator=' ', strip=True)
            
            return text
        except Exception as e:
            logger.error(f"Error extracting text from {url}: {str(e)}")
            return ""
    
    def _extract_title(self, html_content):
        """Extract title from HTML content"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            title_tag = soup.find('title')
            return title_tag.text if title_tag else "No Title"
        except Exception as e:
            logger.error(f"Error extracting title: {str(e)}")
            return "No Title"
    
    def crawl(self, url):
        """Get content from a single URL"""
        self.pages = []
        
        logger.info(f"Fetching content from {url}")
        
        try:
            # Set headers to mimic a real browser
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Connection': 'keep-alive',
            }
            
            try:
                response = requests.get(url, timeout=15, headers=headers, allow_redirects=True)
                response.raise_for_status()
            except requests.exceptions.SSLError:
                logger.warning(f"SSL Error for {url}, trying without verification")
                response = requests.get(url, timeout=15, headers=headers, verify=False, allow_redirects=True)
                response.raise_for_status()
            
            # Extract text content
            text_content = self.extract_text_content(url, response.text)
            
            if text_content:
                self.pages.append({
                    'url': url,
                    'title': self._extract_title(response.text),
                    'content': text_content
                })
                logger.info(f"Successfully extracted content from {url}")
            else:
                logger.warning(f"No content extracted from {url}")
                self.pages.append({
                    'url': url,
                    'title': self._extract_title(response.text),
                    'content': f"No content could be extracted from {url}"
                })
            
        except Exception as e:
            logger.error(f"Error processing URL {url}: {str(e)}")
            self.pages.append({
                'url': url,
                'title': "Error fetching page",
                'content': f"Error fetching content from {url}: {str(e)}"
            })
        
        return self.pages