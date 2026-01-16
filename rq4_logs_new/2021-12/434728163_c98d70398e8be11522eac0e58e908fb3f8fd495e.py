from structures import Request
from tabu_insertion import Tabu_insertion

from colorama import Fore, Back, Style
import json

from objective_function import  Objective_function_imp
from restriction import (Time_window, Distance, Capacity)

from tests import Test

#tabu - colocar pra printar a saida de cada interação e fazer um grafico
#Fazer uma estrutura de variaveis da tabu

#log da execução do programa
#testar sempre, depois de cada incerção
#tá removendo tudo?

'''
com instancias:

Olhar: quantos movimentos não tabu eu to fazendo?
Quantas vezes entra no criterio de aspiração (movimento que melhora)
numero de movimentos que entram repetidos no tabu

fazer grafico

Branch and bound

QUANTIDADE DE CARROS

recebe o melhor vizinho
se o vizinho for melhor que o original, vizinho vira o original
'''

class Tabu_solution:
    def __init__(self, tabu_insertion_number):
        self.solution = None
        self.solution_vehicle = None
        self.solution_value = None
        self.request = None
        self.tabu_insertion_number = tabu_insertion_number
    
    def copy(self):
        new_tabu_solution = Tabu_solution(self.tabu_insertion_number)
        new_tabu_solution.solution = self.solution
        new_tabu_solution.solution_vehicle = self.solution_vehicle
        new_tabu_solution.solution_value = self.solution_value
        new_tabu_solution.request = self.request
        return new_tabu_solution
        

class Tabu_search():
    def __init__(self):
        self.tabu_set = set()
        self.tabu_list = []

        self.restrictions = []
        self.restrictions.append(Time_window())
        self.restrictions.append(Distance())
        self.restrictions.append(Capacity())
        self.objective_function = Objective_function_imp()
        self.tabu_insert = Tabu_insertion(self.restrictions, self.objective_function, self.tabu_set)

    def tabu_move(self, solution):
        tabu_insertion_number = 0
        self.tabu_insert.additional_info = solution.get_additional_info()
        qtd_requests_avalible = solution.qtd_avalible_requests()

        tabu_tenure = qtd_requests_avalible
        max_iteration = qtd_requests_avalible

        global_solution = solution.copy()
        global_solution_value = self.objective_function.solution_value(global_solution)
        initial_solution_value = global_solution_value.copy()
        array_solution_values = []
        array_best_solutions_value = []
        melhora = 0
        i = 0

        while i < max_iteration:
            list_requests = self.non_tabu_requests(global_solution)
            best = Tabu_solution(tabu_insertion_number)
            j = 0
            new_tabu_list = []
            
            for new_request in list_requests:
                tabu_insertion_number += 1
                new = Tabu_solution(tabu_insertion_number)
                if not self.remove_service(new_request, solution):
                    continue
                
                new.solution_vehicle = self.tabu_insert.vehicle_inserted(solution, new_request)
                new.solution = solution

                if new.solution_vehicle is None:
                    continue
                
                #test inicio
                self.write_in_output(new.solution, new_request)
                teste = Test(new.solution)
                teste.verify_vehicles(new.solution)
                #test fim

                new.solution_value = self.objective_function.solution_value(new.solution)

                #log inicio
                solucao = []
                for vehicle in new.solution.get_vehicles():
                    veiculo = []
                    for node in vehicle.get_order():
                        veiculo.append(node.get_id())
                    solucao.append(veiculo)

                log_solution = new.solution_value.copy()
                log_tabu = self.log_tabu_item(new.solution_vehicle, new_request)
                new_tabu_list.append(log_tabu)
                log_solution.update({"tabu_insertion_number": tabu_insertion_number, "vheicle_id, node_id, node_pos": log_tabu, "ordems": solucao})
                array_solution_values.append(log_solution)
                #log fim

                
                if (best.solution is None or
                self.objective_function.best_solution_value(new.solution_value, best.solution_value) == new.solution_value):
                    #print(Fore.YELLOW, log_tabu, Fore.WHITE)
                    best = new.copy()
                    best.request = new_request
                    melhora += 1
                j += 1
            

            if best.solution is not None:
                solution = best.solution

                if len(self.tabu_set) >= tabu_tenure:
                    self.remove_tabu_item(best.request)

                self.add_item_tabu_list( best.solution_vehicle, best.request)
                result_best_solution = self.objective_function.best_solution_value(best.solution_value, global_solution_value)

                if (result_best_solution == best.solution_value):
                    global_solution = best.solution
                    global_solution_value = best.solution_value
                    #log inicio
                    array_best_solutions_value.append(best.solution_value)
                    #log fim
                    
                    i = 0
                else:
                    i += 1
            else:
                i = max_iteration
            #print(Fore.BLUE, i, max_iteration, Fore.WHITE)
            
        print("Quantity of improvments:", melhora)
        self.show_improvment(initial_solution_value, global_solution_value)
        solution = global_solution
        self.add_solution_info_to_json(array_solution_values, array_best_solutions_value)

    def remove_tabu_item(self, best_request):
        print(self.tabu_set.remove(self.tabu_list.pop(0)))
        if type(best_request) is Request:
            self.tabu_set.remove(self.tabu_list.pop(0))
        
    def add_item_tabu_list(self,vehicle, request):
        if type(request) is Request:

            pu = vehicle.pu_last_inserted
            de = vehicle.de_last_inserted
            
            vehicle.pu_last_inserted = None
            vehicle.de_last_inserted = None

            tabu_item1 = self.make_tabu_item(vehicle.get_id(), request.get_pick_up().get_id(), pu)
            self.add_one_item_tabu_list(tabu_item1)
            
            tabu_item2 = self.make_tabu_item(vehicle.get_id(), request.get_delivery().get_id(), de)
            self.add_one_item_tabu_list(tabu_item2)

            return(tabu_item1, tabu_item2)

        else: 
            if request.is_pick_up():
                ls = vehicle.pu_last_inserted
            else:
                ls = vehicle.de_last_inserted
            tabu_item = self.make_tabu_item(vehicle.get_id(), request.get_id(), ls)
            self.add_one_item_tabu_list(tabu_item)

            return tabu_item

    def add_one_item_tabu_list(self, tabu_item):
        if not self.tabu_set.issuperset({tabu_item}):
            self.tabu_set.add(tabu_item)
            self.tabu_list.append(tabu_item)

    def remove_service(self, request,solution):
        if type(request) is Request: 
            return self.remove_request(request, solution)
        else:
            #print("NODE", request.get_pick_up(), request.get_delivery())
            return self.remove_node(request, solution)

    def remove_request(self, request, solution):
        for v in solution.get_vehicles():
            pos_nd = 1
            while pos_nd < len(v.get_order()):
                if v.get_order()[pos_nd].get_id() == request.get_pick_up().get_id():
                    v.get_order().pop(pos_nd)
                elif v.get_order()[pos_nd].get_id() == request.get_delivery().get_id():
                    v.get_order().pop(pos_nd)
                    for restriction in self.restrictions:
                        restriction.aply_restrition(v, 0, self.tabu_insert.additional_info) #importante fazer isso para os testes, talvez retirar depois
                    return True
                else:
                    pos_nd += 1
            
        return False

    def remove_node(self, request, solution):
        for v in solution.get_vehicles():
            for pos_nd in range(1, len(v.get_order())):
                if v.get_order()[pos_nd].get_id() == request.get_id():
                    poped = v.get_order().pop(pos_nd)
                    for restriction in self.restrictions:
                        restriction.aply_restrition(v, 0, self.tabu_insert.additional_info)#vai precisar de algo assim pra tratar quando o last-visited for 0
                    return True
        return False


    def non_tabu_requests(self, solution):
        requests = []

        for v in solution.get_vehicles():
            last_visited = v.get_pos_last_visited()
            qtd_nodes = len(v.get_order())
            for i in range(last_visited+1, qtd_nodes):
                n = v.get_order()[i]
                if n.is_delivery() or self.make_tabu_item(v.get_id(), n.get_id(), i) in self.tabu_set:
                    continue
            
                if n.get_delivery() == 0:
                    requests.append(n)
                else:
                    delivery_pos = v.fast_node_pos(n.get_delivery(), i)
                    r = Request(n, v.get_order()[delivery_pos])
                    requests.append(r)

        return requests

    def make_tabu_item(self, vehicle_id, node_id, node_pos):
        return "(" + str(vehicle_id) + "," + str(node_id) + "," + str(node_pos) + ")"

    def log_tabu_item(self, vehicle, request):
        if type(request) is Request:

            ls_pu = vehicle.pu_last_inserted
            ls_de = vehicle.de_last_inserted
            
            tabu_item1 = self.make_tabu_item(vehicle.get_id(), request.get_pick_up().get_id(), ls_pu)    
            tabu_item2 = self.make_tabu_item(vehicle.get_id(), request.get_delivery().get_id(), ls_de)

            return(tabu_item1, tabu_item2)

        else: 
            if request.is_pick_up():
                ls = vehicle.pu_last_inserted
            else:
                ls = vehicle.de_last_inserted
            tabu_item = self.make_tabu_item(vehicle.get_id(), ls, request.get_id())
            
            return tabu_item

    def add_solution_info_to_json(self, array_solution_values, array_best_solutions_value):
        dictionary = {"um_tabu": {"array_solution_values": array_solution_values, "array_best_solutions_value" : array_best_solutions_value }}
           
        data = {"results": [dictionary]}
        json_object = json.dumps(data, indent = 4)

        with open("sample.json", "w") as outfile:
            outfile.write(json_object)
    
    def write_in_output(self, solution, new_request):
        f = open("output.txt", "a")
        i = 0

        if type(new_request) == Request:
            f.write("graph " + str(i) + str(new_request.get_pick_up().get_id()) +" vehicles:\n")
        else:
            f.write("graph " + str(i) + str(new_request.get_id()) +" vehicles:\n")
        

        for v in solution.get_vehicles():
            f.write("  " + str(v.get_id()) + ":\n")
            for n in v.get_order():
                f.write("    " + str(n.get_id())+ ": " + str(n.get_restriction_info()["start_tw"]) 
                    + " " + str(n.get_restriction_info()["begin_service"]) + " " + 
                    str(n.get_restriction_info()["end_tw"]) + " " + 
                    str(n.get_restriction_info()["begin_service_capacity"]) + " " + 
                    str(n.get_restriction_info()["package_size"]) + "\n")
            f.write("  " + str(v.total_edges(solution.get_additional_info()["edges_distances"])) + "\n")
        i += 1

    def show_improvment(self, initial_solution_value, final_solution_value):
        time_improvment = self.calculate_improvement(initial_solution_value['time'], final_solution_value['time'])
        distance_improvment = self.calculate_improvement(initial_solution_value['distance'], final_solution_value['distance'])

        print("Initial distance:", initial_solution_value['distance'], "final distance:", final_solution_value['distance'])
        print("distance  improvment:", (str(round(distance_improvment, 2)) + "%"))
        print("Initial time:", initial_solution_value['time'], "final time:", final_solution_value['time'])
        print("time improvment:", (str(round(time_improvment, 2)) + "%"))

    def calculate_improvement(self, initial, final):
        if initial == 0:
            return 0
        return (100 - ((100*final)/initial))