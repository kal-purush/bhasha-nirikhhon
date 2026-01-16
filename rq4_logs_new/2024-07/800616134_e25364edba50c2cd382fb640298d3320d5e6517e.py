import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import re
import matplotlib as mpl
import sys
import os
mpl.rcParams['axes.linewidth'] = 1.4 #set the value globally

# Abre o arquivo cpp do BMFinal e retorna o min_erro_j e min_erro_h
# Return dict = {"min_erro_j":j_value, "min_erro_h":h_value}
def extract_formatted_values_from_cpp():
    file_path = "../Scripts/BMfinal.cpp"
    results = {
        "min_erro_j": None,
        "min_erro_h": None
    }

    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

        for line in lines:
            # Search for pattern double min_erro_j
            match_j = re.search(r'double\s+min_erro_j\s*=\s*(-?\d+\.\d+e[-+]?\d+)', line)
            if match_j:
                value_j = float(match_j.group(1))
                results["min_erro_j"] = format(value_j, ".1e")  # Format as scientific notation with minimal precision

            # Search for pattern double min_erro_h
            match_h = re.search(r'double\s+min_erro_h\s*=\s*(-?\d+\.\d+e[-+]?\d+)', line)
            if match_h:
                value_h = float(match_h.group(1))
                results["min_erro_h"] = format(value_h, ".1e")  # Format as scientific notation with minimal precision

            # Break the loop if both patterns are found
            if results["min_erro_j"] is not None and results["min_erro_h"] is not None:
                break

    return results

def MCH_convergencia(N_spins):
    df = pd.read_csv(f"../Results/Erro/erro_sampleN{N_spins}.dat", sep=' ',header=None)
    df.columns = ["MCH", "erro_j","erro_h"]
    return len(df["MCH"])


def test_parms(variavel, N_spins, save):
    min_values = extract_formatted_values_from_cpp()
    MCH_conv = MCH_convergencia(N_spins)
    
    if variavel=="Correlação":
        df = pd.read_csv(f"../Results/Comparativo/{variavel}/Pij_exp_ising_sampleN{N_spins}.dat",sep=' ',header=None)
    elif variavel=="Covariancia":
        df = pd.read_csv(f"../Results/Comparativo/{variavel}/Cij_exp_ising_sampleN{N_spins}.dat",sep=' ',header=None)
    elif variavel=="Magnetização":
        df = pd.read_csv(f"../Results/Comparativo/{variavel}/mag_exp_ising_sampleN{N_spins}.dat",sep=' ',header=None)
    elif variavel=="Tripleto":
        df = pd.read_csv(f"../Results/Comparativo/{variavel}/Tijk_exp_ising_sampleN{N_spins}.dat",sep=' ',header=None)
    elif variavel=="sisjsk":
        df = pd.read_csv(f"../Results/Comparativo/{variavel}/sisjsk_exp_ising_sampleN{N_spins}.dat",sep=' ',header=None)
    elif variavel=="sisj":
        df = pd.read_csv(f"../Results/Comparativo/{variavel}/sisj_exp_ising_sampleN{N_spins}.dat",sep=' ',header=None)
    df.columns = ['x','y']
    
    plt.plot(df['x'], df['y'],'o',ms=10,color='red', label='dado')
    
    #plt.plot(df['x'], df['x'], color='k',label=r'$y = x$')
    

    plt.xlabel(r'$dado$',fontsize=20)
    plt.ylabel(r'$maquina$',fontsize=20)
    plt.text(0.75, 0.1, f'h_min = {min_values["min_erro_j"]}, j_min = {min_values["min_erro_h"]}, MCH_C = {MCH_conv}', transform=plt.gca().transAxes,
         fontsize=18, color='black', ha='center')
    #plt.text(x=)
    plt.grid(alpha=0.4)
    plt.tick_params(labeltop=True, labelright=True, labelsize=14, width=1.4, length=6.0)
    plt.title(f"{variavel} com N = {N_spins}",fontsize=20)
    
    plt.draw()
    xticks = plt.gca().get_xticks()
    yticks = plt.gca().get_yticks()
    step = xticks[1]-xticks[0]
    plt.ylim([df['x'].min() - step, df['x'].max() + step])
    plt.xlim([df['x'].min() - step, df['x'].max() + step])
    straight = [i for i in df['x'].values]
    straight.insert(0,df['x'].min() - step)
    straight.insert(len(straight),df['x'].max() + step)
    plt.plot(straight, straight, color='k',label=r'$y = x$')
    
    if(save==True):
        folder = f"./tests_comparativos/{variable}"
        if not os.path.exists(folder):
            os.makedirs(folder)
        
        j_min = min_values["min_erro_j"]
        h_min = min_values["min_erro_h"]
        
        plt.savefig(folder + f"/{MCH_conv}_j_{j_min}_h_{h_min}.pdf", dpi=300)
    
    #plt.xlim([-0.15, 0.35])
    plt.show()
    
    #return print(xticks,yticks)

# nspins: number of spins, variable: propertie
if __name__ == "__main__":
    nspins = int(sys.argv[1])
    variable = str(sys.argv[2])
    save = bool(sys.argv[3])
    test_parms(variable, nspins, save)    