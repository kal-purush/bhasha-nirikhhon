




"""
import numpy as np
import random

population_size = 20
job_count = 10
max_sublot = [3, 5, 7, 6, 4, 2, 7, 9, 3, 5]

class Population_list(object):
    def __init__(self, LHS, RHS):
        self.LHS = LHS
        self.RHS = RHS

population_list = {"LHS":[], "RHS": []}
for i in range (population_size):
    for j in range(job_count):
        lhs_random_num = []
        for s in max_sublot:
            lhs_random_num = [random.randrange(1, 101, 1) for _ in max_sublot]
            print(lhs_random_num)
            #population_list.update("LHS")
print(population_list)            
"""
import random
import pandas as pd
import numpy as np

population_size = 20
job_count = 10
operation_count = 10
max_sublot = [3, 5, 7, 6, 4, 2, 7, 9, 3, 5]
mc_op = [6, 14, 8, 5, 6, 9, 4, 6, 7, 2]

sequence = [[0, 1, 2, 3, 4, 5, 6, 7, 8, 9], 
[0, 2, 4, 9, 3, 1, 6, 5, 7, 8], 
[1, 0, 3, 2, 8, 5, 7, 6, 9, 4], 
[1, 2, 0, 4, 6, 8, 7, 3, 9, 5], 
[2, 0, 1, 5, 3, 4, 8, 7, 9, 6], 
[2, 1, 5, 3, 8, 9, 0, 6, 4, 7], 
[1, 0, 3, 2, 6, 5, 9, 8, 7, 4], 
[2, 0, 1, 5, 4, 6, 8, 9, 7, 3], 
[0, 1, 3, 5, 2, 9, 6, 7, 4, 8], 
[1, 0, 2, 6, 8, 9, 5, 3, 4, 7]]

def gen_population(population_size, max_sublot, mc_op, job_count, sequence):
    return tuple({'LHS': [gen_lhs(length) for length in max_sublot],
                  'RHS': gen_rhs(np.dot([len(i) for i in sequence], max_sublot), job_count, max_sublot, operation_count, mc_op, sequence)} for _ in range(population_size))


def gen_lhs(length):
    return [round(np.random.uniform(0,1),2) for _ in range(length)]

def gen_rhs(length, job_count, max_sublot, operation_count, mc_op, sequence):
    sequence_len = []
    for i in sequence:
        sequence_len.append(len(i))
    ret = []
    multily = [i * j for i, j in zip([len(i) for i in sequence], max_sublot)]
    job_list = [[k, h] for k, freq in enumerate(multily) for h in range(max_sublot[k]) for i in range(10)]
    random.shuffle(job_list)
    for i in range(length):
        new = []
        a1 = job_list[i][0]
        a2 = job_list[i][1]
        search_for = [a1, a2]
        a3_sequence = []
        a3 = sequence[a1][0]
        try:           
            for i in range(len(ret)):
                if search_for == ret[i][0:2]:
                    a3_sequence.append(i)
                    a3_pre = ret[max(a3_sequence)][2]
                    seq_index = sequence[a1].index(a3_pre)
                    a3 = sequence[a1][seq_index +1]
        except (StopIteration, ValueError, IndexError):
            a3 = sequence[a1][0]
            
        a4 = np.random.randint(mc_op[a3])    
        new.append(a1) #job
        new.append(a2) # sublot
        new.append(a3) #operation
        new.append(a4) # machine
        ret.append(new)
    return ret

print(gen_population(population_size, max_sublot, mc_op, job_count, sequence))

