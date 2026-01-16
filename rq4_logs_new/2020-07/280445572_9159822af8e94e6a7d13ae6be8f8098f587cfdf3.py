import pandas as pd
import numpy as np
from geopy.distance import distance
from ortools.constraint_solver import pywrapcp
from ortools.constraint_solver import routing_enums_pb2


def create_data_model(filename):
    """
    Creates a data model of cities and their pairwise distances
    :param filename:    name of the file to read the data from
    :return:            a dict with list of city names and 2D array of pairwise distances
    """

    def get_distance(from_index, to_index):
        """
        Computes the distance between two locations in kilometers
        :param from_index:  index of the city to start from
        :param to_index:    index of the city to go to
        :return:            the distance between two cities
        """
        return (distance(data.loc[from_index, "Koordinaten"], data.loc[to_index, "Koordinaten"])).km

    # read csv file into pandas dataframe
    data = pd.read_csv(filename)

    # add new column with coordinate tuples (Längengrad, Breitengrad)
    data['Koordinaten'] = list(zip(data.Breitengrad, data.Längengrad))

    # store pairwise distances in 2D array (matrix)
    matrix = np.zeros((len(data.index), len(data.index)))
    for a in range(np.shape(matrix)[0]):
        for b in range(np.shape(matrix)[1]):
            matrix[a][b] = get_distance(a, b)

    data_dict = {'cities': data['msg Standort'],
                 'distance_matrix': matrix}

    return data_dict


def distance_callback(from_index, to_index):
    """
    Returns the distance between the two nodes.
    :param from_index:  city to start from
    :param to_index:    city to go to
    :return:
    """

    # convert from routing variable Index to distance matrix NodeIndex.
    from_node = manager.IndexToNode(from_index)
    to_node = manager.IndexToNode(to_index)

    return data['distance_matrix'][from_node][to_node]


def print_route(manager, routing, solution):
    """
    Prints the optimal route to the console
    """

    print("Kürzeste Route:")
    index = routing.Start(0)    # begin route
    output = ''
    total_distance = 0

    while not routing.IsEnd(index):        # while there are still cities to visit
        output += ' {} ->'.format(data['cities'][manager.IndexToNode(index)])       # get name of current location
        previous_index = index
        index = solution.Value(routing.NextVar(index))                              # go to the next location
        total_distance += routing.GetArcCostForVehicle(previous_index, index, 0)    # add distance to total

    output += ' {}\n'.format(data['cities'][manager.IndexToNode(index)])            # add last city name (depot)
    output += 'Gesamtstrecke: {} Kilometer\n'.format(total_distance)
    # could also get optimal solution directly from the routing model:
    # print('Gesamtstrecke: {} Kilometer'.format(solution.ObjectiveValue()))

    print(output)


if __name__ == '__main__':
    # create data model
    data = create_data_model("msg_standorte_deutschland.csv")
    # create routing index manager
    # inputs: number of cities, number of vehicles/salesmen, depot (start and end location)
    manager = pywrapcp.RoutingIndexManager(len(data['distance_matrix']), 1, 0)
    # create Routing Model
    routing = pywrapcp.RoutingModel(manager)
    # register distance callback with the routing model
    transit_callback_index = routing.RegisterTransitCallback(distance_callback)
    # arc cost evaluator retrieves distances from distance matrix
    routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)
    # define the solution strategy: shortest path
    search_parameters = pywrapcp.DefaultRoutingSearchParameters()
    search_parameters.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
    # solve travelling saleman problem and print solution
    solution = routing.SolveWithParameters(search_parameters)
    if solution:
        print_route(manager, routing, solution)