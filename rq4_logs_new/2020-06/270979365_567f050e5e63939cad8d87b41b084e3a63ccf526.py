from __future__ import print_function
from ortools.constraint_solver import routing_enums_pb2
from ortools.constraint_solver import pywrapcp
import matplotlib.pyplot as plt
import math


def readFile(file_with_routes):
    point_section = 0
    demand_section = 0
    distance_matrix = []
    demand = []
    dist = []
    num_of_vehicles = 10
    vehicle_capacity = 0

    with open(file_with_routes, "r") as ins:
        for line in ins:
            if line.split()[0] == "CAPACITY":
                vehicle_capacity = float(line.split()[2])
            elif line.split()[0] == 'NODE_COORD_SECTION':
                point_section = ''
                continue
            elif line.split()[0] == 'DEMAND_SECTION':
                point_section = 0
                demand_section = ''
                continue
            elif line.split()[0] == 'DEPOT_SECTION':
                demand_section = 0
                continue
            elif line.split()[0] == 'COMMENT':
                if len(line.split()) < 5:
                    num_of_vehicles = 0
                elif line.split()[9].split(',')[0].isdigit() == True:
                    num_of_vehicles = int(line.split()[9].split(',')[0])
                else:
                    num_of_vehicles = 0
            elif type(demand_section) == str:
                var = float(line.split()[1])
                demand.append(var)
            elif type(point_section) == str:
                var1 = float(line.split()[1])
                var2 = float(line.split()[2])
                distance_matrix.append([var1, var2])
    if num_of_vehicles == 0:
        num_of_vehicles = round(round(sum(demand))/round(vehicle_capacity-1))

    for i in range(len(demand)):
        dist_temp = []
        for j in range(len(demand)):
            distance = math.sqrt((distance_matrix[j][0] - distance_matrix[i][0])**2 + (
                distance_matrix[j][1] - distance_matrix[i][1])**2)
            distance = round(distance)
            dist_temp.append(distance)
        dist.append(dist_temp)
    return dist, demand, distance_matrix, vehicle_capacity, num_of_vehicles


def algorithm(dist, demand, distance_matrix, vehicle_capacity, num_of_vehicles,solver,local_search, time_limit):

    def create_data_model(dist, demand, vehicle_capacity, num_of_vehicles):
        """Stores the data for the problem."""
        data = {}
        data['distance_matrix'] = dist
        data['num_vehicles'] = num_of_vehicles
        data['depot'] = 0
        data['demands'] = demand
        data['vehicle_capacities'] = num_of_vehicles*[vehicle_capacity]
        return data

    def print_solution(data, manager, routing, assignment):
        """Prints assignment on console."""
        total_distance = 0
        total_load = 0
        routes = []
        for vehicle_id in range(data['num_vehicles']):
            index = routing.Start(vehicle_id)
            plan_output = 'Route for vehicle {}:\n'.format(vehicle_id)
            route_distance = 0
            route_load = 0
            temp = []

            while not routing.IsEnd(index):
                node_index = manager.IndexToNode(index)
                route_load += data['demands'][node_index]
                plan_output += ' {0} Load({1}) -> '.format(node_index, route_load)
                previous_index = index
                index = assignment.Value(routing.NextVar(index))
                route_distance += routing.GetArcCostForVehicle(
                    previous_index, index, vehicle_id)
                temp.append(node_index)
            temp.append(0)
            plan_output += ' {0} Load({1})\n'.format(manager.IndexToNode(index),
                                                     route_load)
            plan_output += 'Distance of the route: {}m\n'.format(route_distance)
            plan_output += 'Load of the route: {}\n'.format(route_load)
            print(plan_output)
            total_distance += route_distance
            total_load += route_load
            routes.append(temp)
        print('Total distance of all routes: {}m'.format(total_distance))
        print('Total load of all routes: {}'.format(total_load))
        return routes


    def main(solver,local_search, time_limit):
        """Solve the CVRP problem."""
        # Instantiate the data problem.
        data = create_data_model(dist, demand, vehicle_capacity, num_of_vehicles)

        # Create the routing index manager.
        manager = pywrapcp.RoutingIndexManager(len(data['distance_matrix']),
                                               data['num_vehicles'], data['depot'])

        # Create Routing Model.
        routing = pywrapcp.RoutingModel(manager)

        # Create and register a transit callback.
        def distance_callback(from_index, to_index):
            """Returns the distance between the two nodes."""
            # Convert from routing variable Index to distance matrix NodeIndex.
            from_node = manager.IndexToNode(from_index)
            to_node = manager.IndexToNode(to_index)
            return data['distance_matrix'][from_node][to_node]

        transit_callback_index = routing.RegisterTransitCallback(distance_callback)

        # Define cost of each arc.
        routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)

        # Add Capacity constraint.
        def demand_callback(from_index):
            """Returns the demand of the node."""
            # Convert from routing variable Index to demands NodeIndex.
            from_node = manager.IndexToNode(from_index)
            return data['demands'][from_node]

        demand_callback_index = routing.RegisterUnaryTransitCallback(
            demand_callback)
        routing.AddDimensionWithVehicleCapacity(
            demand_callback_index,
            0,  # null capacity slack
            data['vehicle_capacities'],  # vehicle maximum capacities
            True,  # start cumul to zero
            'Capacity')

        # Setting first solution heuristic.
        if local_search == "GREEDY_DESCENT":
            search_parameters = pywrapcp.DefaultRoutingSearchParameters()
            search_parameters.local_search_metaheuristic = (
                routing_enums_pb2.LocalSearchMetaheuristic.GREEDY_DESCENT)
        elif local_search == "GUIDED_LOCAL_SEARCH":
            search_parameters = pywrapcp.DefaultRoutingSearchParameters()
            search_parameters.local_search_metaheuristic = (
                routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH)
            search_parameters.time_limit.seconds = time_limit
            search_parameters.log_search = False
        elif local_search == "TABU_SEARCH":
            search_parameters = pywrapcp.DefaultRoutingSearchParameters()
            search_parameters.local_search_metaheuristic = (
                routing_enums_pb2.LocalSearchMetaheuristic.TABU_SEARCH)
            search_parameters.time_limit.seconds = time_limit
            search_parameters.log_search = True
        if solver == "PATH_CHEAPEST_ARC":
            search_parameters.first_solution_strategy = (
                routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC)
        elif solver == "CHRISTOFIDES":
            search_parameters.first_solution_strategy = (
                routing_enums_pb2.FirstSolutionStrategy.CHRISTOFIDES)


        # Solve the problem.
        assignment = routing.SolveWithParameters(search_parameters)

        # Print solution on console.
        if assignment:
            routes = print_solution(data, manager, routing, assignment)
            x = []
            y = []
            for j in range(num_of_vehicles):
                for i in range(len(routes[j])):
                    x.append(distance_matrix[routes[j][i]][0])
                    y.append(distance_matrix[routes[j][i]][1])
                    plt.plot(distance_matrix[routes[j][i]][0],distance_matrix[routes[j][i]][1], marker=".")
                plt.ion()
                plt.plot(x, y)
                x = []
                y = []
            plt.plot(distance_matrix[0][0],distance_matrix[0][1], marker="8", color='black')
            plt.show()

    if __name__ == '__main__':
        main(solver,local_search, time_limit)


def get_input_from_user():
    file_to_read = input('Write file name to read: E-n22-k4, E-n23-k3, P-n16-k8, X-n110-k13, X-n115-k10, X-n120-k6, Golden_1, Golden_2\n')
    file_to_read = file_to_read+".txt"
    p = False
    while p == False:
        solver = input("Chose algorithm, write shortcut: PC - path cheapest, C - Christofides algorithm\n")
        if solver == "PC" or solver == "pc" or solver == 'Pc' or solver == "pC":
            chosed_solver = "PATH_CHEAPEST_ARC"
            p = True
        elif solver == "C" or solver == "c":
            chosed_solver = "CHRISTOFIDES"
            p = True
        else:
            print("Wrong format")
            p = False
    p = False
    while p == False:
        local_search_strategy = input(
            'Chose metaheuristics, write shorcut: GD - greedy descent ,GLS - guided local search, T - tabu search \n')
        if local_search_strategy == "GD" or local_search_strategy == "gd" or local_search_strategy == "Gd" or local_search_strategy == "gD":
            chosed_local_search_strategy = "GREEDY_DESCENT"
            p = True
        elif local_search_strategy == "gls" or local_search_strategy == "GLS" or local_search_strategy == "Gls":
            chosed_local_search_strategy = "GUIDED_LOCAL_SEARCH"
            p = True
        elif local_search_strategy == "t" or local_search_strategy == "T":
            chosed_local_search_strategy = "TABU_SEARCH"
            p = True
        else:
            print("Wrong format")
            p = False
    p = False
    while p == False:
        time_limit = input("Time limit for finding solution in seconds:  ")
        if time_limit.isdigit() == False:
            print("Wrong format")
        else:
            time_limit = int(time_limit)
            p = True

    print("Calculaiting ...")
    return file_to_read, chosed_solver, chosed_local_search_strategy, time_limit


def main_program():
    file_to_read, solver, local_search, time_limit = get_input_from_user()
    dist, demand, distance_matrix, vehicle_capacity, num_of_vehicles = readFile(file_to_read)
    algorithm(dist, demand, distance_matrix, vehicle_capacity, num_of_vehicles, solver,local_search, time_limit)


main_program()
r=input()