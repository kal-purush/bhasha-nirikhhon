#!/usr/bin/python3
# -*- coding: utf-8 -*-

import sys
import csv
import calcoohija

calc = calcoohija.CalculadoraHija()

with open(sys.argv[1]) as csvarchivo:
    lineas = csv.reader(csvarchivo)
    try:
        for linea in lineas:
            print(linea)
            operacion = linea[0]
            print(operacion)
            if operacion == "suma":
                suma = 0
                for operandos in linea[1:]:
                    suma = calc.plus(suma, int(operandos))
            if operacion == "resta":
                rest = int(linea[1])
                for operandos in linea[2:]:
                    rest = calc.minus(rest, int(operandos))
            if operacion == "multiplica":
                mult = int(linea[1])
                for operandos in linea[2:]:
                    mult = calc.mult(mult, int(operandos))
            if operacion == "divide":
                div = int(linea[1])
                for operandos in linea[2:]:
                    div = calc.div(div, int(operandos))

        print(suma)
        print(rest)
        print(mult)
        print(div)
    except ValueError:
        sys.exit("Error: Non numerical parameters")