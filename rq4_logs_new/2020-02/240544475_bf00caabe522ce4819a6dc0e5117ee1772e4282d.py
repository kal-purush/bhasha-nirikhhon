#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan 31 11:40:13 2020

@author: mattbright
"""

import re
import sympy
import numpy as np


def Input_Check(code):
    """Checks that any input satisfies the requirements of a textile code"""
    check = False

    """ Check for correct overall structure """
    sanity_check = re.compile(r'([hvou]\d[+-])+')
    m = sanity_check.search(code)
    format_check = (m.group() == code and code[:2] == 'h1'
                    and 'v' in code)

    """ Check for matching crossing numbers """
    cross_num_list = [int(code[i+1]) for i in range(len(code)) if
                      code[i] == "o" or code[i] == "u"]
    cross_num_count_list = [cross_num_list.count(j)
                            for j in set(cross_num_list)]
    cross_num_check = (len(set(cross_num_count_list)) == 1 and
                       2 in set(cross_num_count_list))

    """ Check crossing signs match """
    sgn_list = [code[i+1] + code[i+2] for i in range(len(code)) if
                code[i] == "o" or code[i] == "u"]
    sgn_list_check = (len(sgn_list)/len(set(sgn_list)) == 2)

    if not format_check:
        print(code, " isn't a valid paragraph.")
    elif not cross_num_check:
        print(code, " has a crossing which doesn't appear twice.")
    elif not sgn_list_check:
        print(code, " has an inconsistently oriented crossing.")
    else:
        check = True

        return check


def Car_Poly_Convert(code):
    """Translates original code so that symbols match generators
       and abelianised deck transformation group elements"""

    new_code = re.sub(r'h(\d)[+-]', 'y', code)
    new_code = re.sub(r'v(\d)[+-]', 'x', new_code)
    new_code = re.sub('u', '', new_code)
    new_code = re.sub(r'o(\d)[+-]', r'\1', new_code)
    for i in range(len(new_code)):
        if new_code[i] == 'o':
            new_code.replace(new_code[i+2], '')
            new_code.replace(new_code[i], '')

    return(new_code)
    print(new_code)


def Make_Relator(code):
    """
    Makes an ordered list of just crossing numbers
    """
    rel_list = [i for i in code if i not in ['+', '-', 'x', 'y']]
    rel_set = set([int(i) for i in rel_list])
    return(rel_set)


def Make_Generator(code, i):
    """
    Generates a substring of the input code corresponding to a formal arc
    """

    """find the undercrossing symbol i+ for a given i"""
    search_list = [str(i+'+'), str(i+'-')]
    start_index = max([code.find(j) for j in search_list])

    """generator starts with the i+ symbol"""
    generator = code[start_index]+code[start_index+1]
    k = (start_index+2) % (len(code))

    """Iterate cyclically through string until another undercrossing
       symbol j+ is reached"""
    while code[k] not in ['+', '-']:
        generator += code[k]
        k = (k+1) % (len(code))

    """Add the final crossing orientation symbol"""
    generator += code[k]
    return(generator)


def Make_Generator_List(code):
    """
    Creates a dictionary that associates a relator labelled by its
    crossing number i to a generator represented by a formal arc
    beginning with i+/-
    """

    Generator_List = [Make_Generator(code, str(k)) for k in Make_Relator(code)]
    return(Generator_List)


def Alexander_Matrix(arclist):
    """
    Builds the Alexander Matrix
    """
    x, y, t = sympy.symbols('x y t')
    listindex = [i[0] for i in arclist]
    loop_array = ([[0 for i in arclist] for i in arclist])
    over_deckmult = [[0 for i in arclist] for i in arclist]
    alldeck_list = [[0 for i in arclist] for i in arclist]

    """
    Deck transform multipliers for crossings at ends of formal arcs
    """
    
    for i in arclist:
        if i[-1] == '+':
            base = t
        else:
            base = -1
        mult = 1
        for k in i:
            if k in ['x', 'y']:
                mult = mult * sympy.sympify(k)
        alldeck_list[int(i[-2])-1][arclist.index(i)] += (base * mult)
        

    """
    Deck transform multipliers for overcrossings
    """
    for i in arclist:
        for j in range(0, len(i)-1):
            if i[j] in listindex and i[j+1] not in ['+', '-']:
                base = 1-t
                mult = 1
                for k in i[:j]:
                    if k in ['x', 'y']:
                        mult = mult * sympy.sympify(k)
                over_deckmult[int(i[j])-1][int(i[0])-1] += (base * mult)           

    """
    Loop multipliers for crossings at beginning of formal arcs
    """
    for i in arclist:
        if i[0] == listindex[arclist.index(i)]:
            if i[1] == '+':
                loop_array[int(i[0]) - 1][int(i[0]) - 1] -= 1
            else:
                loop_array[int(i[0]) - 1][int(i[0]) - 1] += t

    total_array = (np.add(over_deckmult, np.add(loop_array, alldeck_list)))
    symbolic_array = sympy.Matrix(total_array)
    return symbolic_array


def Make_Alex_Poly(code):
    x, y, t = sympy.symbols('x y t')
    m = Alexander_Matrix(Make_Generator_List(code))
    raw_poly = sympy.expand(sympy.simplify(m.det()))
    tidy_poly = sympy.collect(raw_poly, t)
    return tidy_poly


my_code = input('Please input your code: ')

while not Input_Check(my_code):
    my_code = input("Please correct your code and try again: ")

x, y, t = sympy.symbols('x y t')
my_gen_list = Make_Generator_List(Car_Poly_Convert(my_code))
m = Alexander_Matrix(my_gen_list)
raw_poly = sympy.expand(sympy.simplify(m.det()))
tidy_poly = sympy.collect(raw_poly, t)
print("Relators: ", Make_Relator(Car_Poly_Convert(my_code)))
print("Formal Arcs (Generators): ", my_gen_list)
print("Alexander Matrix:, ", m)
print("0th Alexander Polynomial: ", tidy_poly)
"""


para_file = open('para_file.txt', 'w')

input_file = open('Realizable_Gauss_Paragraphs_1_1_3.txt', 'r')


for line in input_file:
    if line[0] == 'h':
        convert = Car_Poly_Convert(line.strip())
        out = Make_Alex_Poly(convert)
        para_file.write(str(out)+'\n')

para_file.close()

input_file.close()
"""
