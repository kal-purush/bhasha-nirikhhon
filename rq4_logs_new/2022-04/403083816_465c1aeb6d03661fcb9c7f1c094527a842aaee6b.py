'''
format performs all the necessary formatting amd simplication including rounding and other

'''

import numpy as np
import streamlit as st

def for_weight(uom, input):
    # uom - Unit of measure
    if uom == 'Pounds':
        lb = round(input, 2)
        kg = round(input/2.2, 2)
    elif uom == 'Kilograms':
        lb = round(input*2.2,2)
        kg = round(input,2)
    print(f'{lb} pounds or {kg} kilograms')
    return lb, kg