import sys
import time
import client_config
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import networkx as nx
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from community import community_louvain
import collections
import itertools

client_id = client_config.client_id
client_secret = client_config.client_secret
client_credentials_manager = spotipy.oauth2.SpotifyClientCredentials(client_id, client_secret)
spotify = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

def search_artist(artist_name):
    try: 
        for market in ['US', 'JP']:
            artist_search_results = spotify.search(q=artist_name, market=market, type='artist')
            if len(artist_search_results['artists']['items']) != 0:
                err_msg = ''
                break
            else:
                err_msg = 'Artist not found'
        if len(err_msg) != 0:
            raise ValueError(err_msg)
        else:
            pass
    except ValueError as err:
        sys.exit(err)

    if len(artist_search_results['artists']['items']) == 1:
        artist_index = 0
    else:
        print('-'*50)
        print('Input the index of "{}".'.format(artist_name))
        print('-'*50)
        for i, artist_search_result in enumerate(artist_search_results['artists']['items']):
            try:
                print("[{}] {} - popularity: {}, ImageURL: {}".format(i, artist_search_result['name'],
                    artist_search_result['popularity'], artist_search_result['images'][0]['url']))
            except IndexError:
                print("[{}] {} - popularity: {}".format(i, artist_search_result['name'],
                    artist_search_result['popularity']))
        print('-'*50)
        artist_index = int(input('Artist Index: '))
    try:
        artist_name = artist_search_results['artists']['items'][artist_index]['name']
        artist_id = artist_search_results['artists']['items'][artist_index]['id']
    except IndexError:
        sys.exit('Input the correct artist name.')
    return artist_name, artist_id

def search_related_artists(artist_id):
    related_artist_names = []
    related_artist_popularities = []
    related_artist_ids = []
    related_artist_genres = []

    related_artists = spotify.artist_related_artists(artist_id)
    for artist in related_artists['artists'][:10]:
        name = artist['name']
        popularity = artist['popularity']
        unique_id = artist['id']
        genres = artist['genres']
        related_artist_names.append(name)
        related_artist_popularities.append(popularity)
        related_artist_ids.append(unique_id)
        related_artist_genres.append(genres)
    return related_artist_names, related_artist_ids

def plot_network(artists):
    G = nx.Graph()
    [G.add_edges_from(artist) for artist in artists]
    plt.figure(figsize=(10, 7))
    pos = nx.spring_layout(G)
    partition = community_louvain.best_partition(G)
    betcent = nx.communicability_betweenness_centrality(G)
    node_size = [10000 * size for size in list(betcent.values())]
    nx.draw_networkx_nodes(G, pos, node_color=[partition[node] for node in G.nodes()], alpha=0.9, node_size=node_size)
    nx.draw_networkx_labels(G, pos, font_size=7)
    nx.draw_networkx_edges(G, pos, alpha=0.5)
    plt.axis('off')
    plt.show()

if __name__ == "__main__":
    reference_artist_name = input('Artist Name: ')
    reference_artist_name, reference_artist_id = search_artist(reference_artist_name)
    iter_names, iter_ids = search_related_artists(reference_artist_id)
    artists = [[(reference_artist_name, related_artist_name) for related_artist_name in iter_names]]
    for reference_artist_name, reference_artist_id in zip(iter_names, iter_ids):
        time.sleep(0.3)
        related_artist_names, _ = search_related_artists(artist_id=reference_artist_id)
        if len(related_artist_names) != 0:
            artists.append([(reference_artist_name, related_artist_name) for related_artist_name in related_artist_names])
    plot_network(artists)
    