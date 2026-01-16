import json
import urllib.parse
import urllib.request
from sys import argv

def read_webhose_key():
    """
    Reads the Webhose API key from a file called 'search.key'
    Returns either None (no key), or a string representing the key
    Search.key is in .gitignore so it won't be commited to version control
    """

    webhose_api_key = None
    try:
        with open('search.key', 'r') as f:
            webhose_api_key = f.readline().strip()
    except:
        raise IOError('search.key not found')

    return webhose_api_key

def run_query(search_terms, size=10):
    """
    search_terms = string with query
    size = desired number of results

    run_query searches webhose for [search_terms]
    and returns [size] results. Each result consists
    of a title, link, and summary
    """

    webhose_api_key = read_webhose_key()

    if not webhose_api_key:
        raise KeyError('Webhose API key not found')

    # base URL for webhose API
    root_url = 'http://webhose.io/search'

    # format query string for URLs
    query_string = urllib.parse.quote(search_terms)

    # use str.format() method to construct a complete
    # URL for the webhose API.
    search_url = ('{root_url}?token={key}&format=json&q={query}'
                  '&sort=relevancy&size={size}').format(
                    root_url=root_url,
                    key=webhose_api_key,
                    query=query_string,
                    size=size)

    results = []

    try:
        # Connect to webhose API and convert response
        # into a Python Dictionary.
        response = urllib.request.urlopen(search_url).read().decode('utf-8')
        json_response = json.loads(response)
        print(json.dumps(json_response,sort_keys=True, indent=4))


        # loop through json_response converting each json object into a 
        # dictionary and add it to the list of results
        for post in json_response['posts']:

            results.append({'title': post['title'],
                            'link': post['url'],
                            'summary': post['text'][:200]})
    except:
        print("Error when querying Webhose API")

    # return results
    return results

def main():
    """
    run query with command line arguments for testing
    """
    results = run_query(search_terms='Donald Trump', size=10)
    
    for result in results:
        print('Title = ' + result['title'] + '\n')
        print('URL = ' + result['link'] + '\n')
        print('Summary = ' + result['summary'] + '\n\n\n')

if __name__ == '__main__':
    main()