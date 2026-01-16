import numpy as np
import math
from scipy.integrate import simps
import sys
import os

print("\n")
print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
print(" Script for construction of GF from DFT DOS (Sigma is 0)")
print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
print("\n")

path_to_file = "/Users/witcher/workspace/CoCu15/LDA/"
filename     = path_to_file + "cub.dos"
beta         = 40
idelta       = 1j * math.pi / beta
num_of_orbs  = 10
number_of_mats_freqs = 200

def main ():
    f = open(filename, "r")
    lines = f.readlines()

    freq    = []
    ro_t2g  = []
    ro_eg   = []
    Delta_t2g  = []
    Delta_eg   = []
    Delta_t2g_imag  = []
    Delta_eg_imag   = []
    

    for line in lines:
        # ro is density of states from DFT calculation
        freq.append(float(line.split()[0]))
        ro_t2g.append(float(line.split()[1]))
        ro_eg.append(float(line.split()[-1]))
    f.close()

    array_length_freq   = len(freq)
    array_length_dos    = len(ro_t2g)
    
    if (array_length_freq == array_length_dos):
        a = 1
    else:
        os.exit()
    
    #
    # Real frequencies Green's functions
    #
    GF_t2g  = construct_GF_from_DOS(freq, ro_t2g, "t2g")
    GF_eg   = construct_GF_from_DOS(freq, ro_eg,  "eg")

    for i in range(array_length_freq):
        Delta_t2g.append(-(1.0/GF_t2g[i]).imag)
        Delta_eg.append(-(1.0/GF_eg[i]).imag)

    save_Delta_to_file(freq, Delta_t2g, "Delta_t2g")
    save_Delta_to_file(freq, Delta_eg, "Delta_eg")
    
    #
    # Matsubara frequencies Green's functions
    #
    
    mats_fr = matsubara_frequencies(number_of_mats_freqs)
    
    GF_t2g_imag  = construct_GF_from_DOS_matsubara(freq, mats_fr, number_of_mats_freqs, ro_t2g, "t2g_matsubara")
    GF_eg_imag   = construct_GF_from_DOS_matsubara(freq, mats_fr, number_of_mats_freqs, ro_eg,  "eg_matsubara")
    
    for i in range(number_of_mats_freqs):
           Delta_t2g_imag.append(1j * mats_fr[i] - 1.0/GF_t2g_imag[i])
           Delta_eg_imag.append(1j * mats_fr[i] - 1.0/GF_eg_imag[i])
           
    save_Delta_to_file(mats_fr, Delta_t2g_imag, "Delta_t2g_matsubara")
    save_Delta_to_file(mats_fr, Delta_eg_imag, "Delta_eg_matsubara")

def matsubara_frequencies(number_of_frequencies):
    mats_freq = []
    for i in range(number_of_frequencies):
        fr  = (2 * i + 1) * math.pi / beta
        mats_freq.append(fr)
    return mats_freq

def save_GF_to_file(freq, func, pointer):
    # function is complex
    f = open("GF_" + pointer + ".dat", "w")
    length = len(freq)
    for i in range(length):
        f.write(str(freq[i]))
        f.write('\t')
        f.write(str(func[i].real))
        f.write('\t')
        f.write(str(func[i].imag))
        if (i != length):
            f.write('\n')
    f.close()

def save_Delta_to_file(freq, func, pointer):
    # function is complex
    f = open(pointer + ".dat", "w")
    length = len(freq)
    for i in range(length):
        f.write(str(freq[i]))
        f.write('\t')
        f.write(str(func[i].real))
        f.write('\t')
        f.write(str(func[i].imag))
        if (i != length):
            f.write('\n')
    f.close()

def construct_GF_from_DOS(freq, dos, pointer):
    array_length_freq   = len(freq)
    
    GF = []
    total_occup = simps(dos, freq)
    
#    freqs =
    function_for_this_freq = []
    
    for i in range(array_length_freq):
        for j in range(array_length_freq):
            function_for_this_freq.append(dos[j]/(freq[i] + idelta - freq[j]))
        value = simps(function_for_this_freq, freq)
        GF.append(value)
        function_for_this_freq = []
    save_GF_to_file(freq, GF, pointer)
    return GF
    
def construct_GF_from_DOS_matsubara(real_freq, mats_freq, number_of_mats_freqs, dos, pointer):
    
    array_length_freq = len(real_freq)
    
    GF = []
    total_occup = simps(dos, real_freq)
            
    function_for_this_freq = []

    for i in range(number_of_mats_freqs):
        for j in range(array_length_freq):
            function_for_this_freq.append(dos[j]/(mats_freq[i] * 1j  - real_freq[j]))
        value = simps(function_for_this_freq, real_freq)
        GF.append(value)
        function_for_this_freq = []
    save_GF_to_file(mats_freq, GF, pointer)
    return GF

main()