import recommender_core.misc
import connection_provider as cp
# import settings
import numpy as np
from sklearn.cluster import DBSCAN

RELEVANT_COLD_START_TAGS = ['restaurant', 'cafe', 'tourist']

def coldstart_recommendations(city):
    # We want to find a good coldstart without requiring any user data.
    # For this purpose we assume that interesting POIs are surrounded by
    # restaurants, cafes and souvenir shops. Hence, we are looking for
    # clusters around the city. We use DBSCAN for this purpose.

    print(f'Creating coldstart recommendations for city {city}.')
    city_data = query_city_data(city)

    coordinates = {}
    for point in city_data:
        print(point['TYPE'])
        coordinates[(point["LON"], point["LAT"])] = point

    # labels identify the clusters
    # TODO try different parameters - should be similar for multiple cities, otherwise
    # we have to solve a optimization problem as follows:
    # find parameters which maximize clusters while minimizing noise for regions of high
    # centrality.
    db = DBSCAN(eps=0.001, min_samples=3).fit(np.asarray(list(coordinates.keys())))

    labels = db.labels_
    print(len(set(db.labels_)))
    print(db.labels_)

    # label all the nodes with their appropriate cluster - list(tuple(data_point, cluster_id))
    labeled_data = label_data(city_data, distance_matrix, labels)

    # estimate importance by cluster size
    value_estimation = estimate_importance(labeled_data)

    attractions = extract_attractions(labeled_data)

def query_city_data(city_name):
    # create sql query for this
    c = cp.get().cursor()

    c.execute("select * from NODES as n, TOURISM as t, AMENITIES as other where city=? and (n.NODE_ID = t.NODE_ID or n.NODE_ID = other.NODE_ID) COLLATE NOCASE", (city_name,))

    return c.fetchall()


