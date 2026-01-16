import pandas as pd
import numpy as np


# Clase que extraerá los indicies de los excels paths
class IndicesDesdeExcel():
    def __init__(self, xls_path: str):
        self.df = pd.read_excel(xls_path, usecols='A:H', skiprows=6)
        self.df = self.df.dropna()

    #def a1(self):
    #    ## Intrachromosomal Asymmetry, Romero-Zarco 1986 ##
    #
    #    # Ordenar df con la idea de obtener los pares homólogos
    #    df_ordenado = self.df.sort_values('Arm Ratio (L/S)')
    #    brazos_cortos_pares_homologos = []
    #    brazos_largos_pares_homologos = []
    #    n = len(self.df)//2
    #    for i in range(n):
    #        brazos_cortos_pares_homologos.append(np.mean(df_ordenado['Short arm'].iloc[2*i:2*i + 2]))
    #        brazos_largos_pares_homologos.append(np.mean(df_ordenado['Long arm'].iloc[2*i:2*i + 2]))
    #    b_div_B = [b / B for b, B in zip(brazos_cortos_pares_homologos, brazos_largos_pares_homologos)]
    #
    #    self.brazos_cortos_pares_homologosa1 = brazos_cortos_pares_homologos
    #    self.brazos_largos_pares_homologosa1 = brazos_largos_pares_homologos
    #    return 1 - (np.sum(b_div_B) / n)

    def a2(self, ddof=1):
        ## Romero Zarco, 1986 ##

        media_largos_cromosomas = np.mean(self.df['Length each'])        
        desviacion_estandar_largos_cromosomas = np.std(self.df['Length each'], ddof=ddof)
        return desviacion_estandar_largos_cromosomas/media_largos_cromosomas
    
    def askp(self):
        ## Karyotype Asymmetry Index Percentage, Arano y Saito, 1980 ##

        ask = np.sum(self.df['Long arm']) / np.sum(self.df['Length each'])
        return ask*100

    def cvci(self, ddof=1):
        ## Coeficient of Variation in the Centromeric Index, Paszko, 2006 ##

        media_indice_centromerico = np.mean(self.df['Cent. Index (S/(L+S))'])
        desviacion_estandar_indice_centromerico = np.std(self.df['Cent. Index (S/(L+S))'], ddof=ddof)
        return desviacion_estandar_indice_centromerico/media_indice_centromerico * 100

    def cvcl(self, ddof=1):
        ## Coeficient of Variation of Chromosome Length, Peruzzi y Eroglu, 2013 ##

        media_largos_cromosomas = np.mean(self.df['Length each'])        
        desviacion_estandar_largos_cromosomas = np.std(self.df['Length each'], ddof=ddof)
        return desviacion_estandar_largos_cromosomas/media_largos_cromosomas * 100
    
    def mca(self):
        ## Mean Centromeric Asymmetry, Peruzzi y Eroglu, 2013 ##

        asimetria_centromerica = (self.df['Long arm'] - self.df['Short arm']) / (self.df['Long arm'] + self.df['Short arm'])
        return np.mean(asimetria_centromerica) * 100

    def syi(self):
        ## Symmetric Index, Greihuber y Speta, 1976 ##

        return np.mean(self.df['Short arm']) / np.mean(self.df['Long arm']) * 100

    def tfp(self):
        ## Total Form Percentage, Huziwara, 1962 ##

        tf = np.sum(self.df['Short arm']) / np.sum(self.df['Length each'])
        return tf*100

    def calcular_indices(self, indices):
        dicc = dict()
        for indice in indices:
            if indice == u'A\u2082':
                dicc = dict(dicc, **{indice: self.a2()})
            elif indice == 'Ask%':
                dicc = dict(dicc, **{indice: self.askp()})
            elif indice == 'CVCI':
                dicc = dict(dicc, **{indice: self.cvci()})
            elif indice == 'CVCL':
                dicc = dict(dicc, **{indice: self.cvcl()})
            elif indice == 'MCA':
                dicc = dict(dicc, **{indice: self.mca()})
            elif indice == 'Syi':
                dicc = dict(dicc, **{indice: self.syi()})
            elif indice == 'TF%':
                dicc = dict(dicc, **{indice: self.tfp()})
        return dicc