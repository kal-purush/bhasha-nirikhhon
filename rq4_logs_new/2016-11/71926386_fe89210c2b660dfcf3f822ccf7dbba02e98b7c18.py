import requests
import pprint as pp

def get_movie_data(query_params):
    ombd_api = 'http://www.omdbapi.com'
    resp = requests.get(ombd_api, params=query_params)
    return resp.json()

def main(args):
    pp.pprint(get_movie_data({'t': args[1]}))

if __name__=='__main__':
    import sys
    main(sys.argv)