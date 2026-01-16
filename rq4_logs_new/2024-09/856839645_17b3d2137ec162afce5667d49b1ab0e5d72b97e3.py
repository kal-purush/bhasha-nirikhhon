import requests
import xml.etree.ElementTree as ET
import json

# 1. Function to fetch data from arXiv API
def fetch_arxiv_data(search_query, max_results=5):
    base_url = 'http://export.arxiv.org/api/query?'
    query = f'search_query={search_query}&max_results={max_results}'
    response = requests.get(base_url + query)
    
    if response.status_code == 200:
        root = ET.fromstring(response.content)
        papers = []
        for entry in root.findall('{http://www.w3.org/2005/Atom}entry'):
            title = entry.find('{http://www.w3.org/2005/Atom}title').text
            abstract = entry.find('{http://www.w3.org/2005/Atom}summary').text
            author = [author.find('{http://www.w3.org/2005/Atom}name').text for author in entry.findall('{http://www.w3.org/2005/Atom}author')]
            published_date = entry.find('{http://www.w3.org/2005/Atom}published').text
            link = entry.find('{http://www.w3.org/2005/Atom}id').text
            papers.append({'title': title, 'abstract': abstract, 'author': author, 'link': link, 'published_date': published_date})
        return papers
    else:
        print("Failed to fetch data from arXiv")
        return []

# 2. Function to fetch data from CrossRef API
def fetch_crossref_data(search_query, max_results=5):
    base_url = 'https://api.crossref.org/works'
    params = {
        'query': search_query,
        'rows': max_results
    }
    response = requests.get(base_url, params=params)
    
    if response.status_code == 200:
        data = response.json()
        papers = []
        for item in data['message']['items']:
            title = item['title'][0] if 'title' in item and item['title'] else 'No title available'
            abstract = item.get('abstract', 'No abstract available')
            author = [author.get('name', 'Unknown') for author in item.get('author', [])]
            published_date = item['created']['date-time'] if 'created' in item else 'Unknown date'
            link = item.get('URL', 'No link available')
            papers.append({'title': title, 'abstract': abstract, 'author': author, 'link': link, 'published_date': published_date})
        return papers
    else:
        print("Failed to fetch data from CrossRef")
        return []

# 3. Function to fetch data from CORE API
def fetch_core_data(search_query, max_results=5):
    api_key = "YOUR_CORE_API_KEY"  # Replace with your CORE API key
    base_url = f'https://api.core.ac.uk/v3/search/works'
    params = {
        'query': search_query,
        'limit': max_results
    }
    headers = {
        'Authorization': f'Bearer {api_key}'
    }
    response = requests.get(base_url, params=params, headers=headers)

    if response.status_code == 200:
        data = response.json()
        papers = []
        for item in data['results']:
            title = item.get('title', 'No title available')
            abstract = item.get('abstract', 'No abstract available')
            author = item.get('authors', [])
            published_date = item.get('publishedDate', 'Unknown date')
            link = item.get('downloadUrl', 'No link available')
            papers.append({'title': title, 'abstract': abstract, 'author': author, 'link': link, 'published_date': published_date})
        return papers
    else:
        print("Failed to fetch data from CORE")
        return []

# 4. Function to fetch data from DOAJ API
def fetch_doaj_data(search_query, max_results=5):
    base_url = 'https://doaj.org/api/v2/search/articles'
    params = {
        'q': search_query,
        'pageSize': max_results
    }
    response = requests.get(base_url, params=params)

    if response.status_code == 200:
        data = response.json()
        papers = []
        for item in data['results']:
            title = item['bibjson'].get('title', 'No title available')
            abstract = item['bibjson'].get('abstract', 'No abstract available')
            author = [author['name'] for author in item['bibjson'].get('author', [])]
            published_date = item['created_date'] if 'created_date' in item else 'Unknown date'
            link = item['bibjson'].get('link', [{'url': 'No link available'}])[0]['url']
            papers.append({'title': title, 'abstract': abstract, 'author': author, 'link': link, 'published_date': published_date})
        return papers
    else:
        print("Failed to fetch data from DOAJ")
        return []

# Function to fetch papers from all APIs concurrently using ThreadPoolExecutor
from concurrent.futures import ThreadPoolExecutor

def fetch_all_papers(search_query, max_results):
    papers = []
    with ThreadPoolExecutor() as executor:
        future_arxiv = executor.submit(fetch_arxiv_data, search_query, max_results)
        future_crossref = executor.submit(fetch_crossref_data, search_query, max_results)
        future_core = executor.submit(fetch_core_data, search_query, max_results)
        future_doaj = executor.submit(fetch_doaj_data, search_query, max_results)

        for future in [future_arxiv, future_crossref, future_core, future_doaj]:
            result = future.result()
            if result:
                papers.extend(result)
    
    return papers