# -*- coding: utf-8 -*-
"""
@date: Created on Mon Oct 25 09:28:18 2021
@author: Carlos Puerto-Santana at Aingura IIoT, San Sebastian, Spain
@Licence: CC BY-NC-ND 4.0
"""
from pathlib import Path
root = str(Path().absolute())
library = root+r"\models"
from AR_ASLG_HMM import AR_ASLG_HMM           # Cite: C. Puerto-Santana, P. Larranaga and C. Bielza, "Autoregressive Asymmetric Linear Gaussian Hidden Markov Models," in IEEE Transactions on Pattern Analysis and Machine Intelligence, doi: 10.1109/TPAMI.2021.3068799.
from AR_MOG_HMM import AR_MOG_HMM             # Cite: B. H. Juang, ”Maximum likelihood estimation for mixturemultivariate stochastic observations of Markov chains,” AT&TTech. I., vol. 64, no. 6, pp. 1235-1249, July-Aug. 1985. 
from AR_MOG_RHMM import AR_MOG_RHMM           # Cite: Biing-Hwang Juang and L. Rabiner, "Mixture autoregressive hidden Markov models for speech signals," in IEEE Transactions on Acoustics, Speech, and Signal Processing, vol. 33, no. 6, pp. 1404-1413, December 1985, doi: 10.1109/TASSP.1985.1164727.
from LFS_AsHMM import AsFS_AR_ASLG_HMM        # Cite: C. Puerto-Santana, P. Larranaga and C. Bielza, "Features Saliencies in Asymmetric Hidden Markov Models"
from BMM import BMM                           # Cite: Jeff A. Bilmes, Buried Markov models: a graphical-modeling approach to automatic speech recognition, Computer Speech & Language, Volume 17, Issues 2–3, 2003,
from DHMM import DHMM                         # Cite: L. E. Baum and T. Petrie, “Statistical inference for probabilistic functions of finite state Markov chains,”Ann. Math. Stat.,
from FS_AsHMM import FS_AsHMM                 # Cite: C. Puerto-Santana, P. Larranaga and C. Bielza, "Features Saliencies in Asymmetric Hidden Markov Models"
from FS_HMM import FS_HMM                     # Cite: S. Adams, P. A. Beling and R. Cogill, "Feature Selection for Hidden Markov Models and Hidden Semi-Markov Models," in IEEE Access, vol. 4, pp. 1642-1657, 2016, doi: 10.1109/ACCESS.2016.2552478.
from LMSAR import LMSAR                       # Cite: J. Hamilton, "A New Approach to the Economic Analysis of Nonstationary Time Series and the Business Cycle", Econometrica, 1989
from VAR_MOG_HMM import VAR_MOG_HMM           # Cite: A. Poritz, "Linear predictive hidden markov models and the speech signal", in IEEE Xplore
from CT_DHMM import CT_DHMM
from KDE_AsHMM import KDE_AsHMM               # Cite: ...
#%% 
class HMM:
    def __init__(self,tipo):
        """
        Interface to use the current programmed HMMs. See the documentation of each model to see the possible functions and attributes

        Parameters
        ----------
        tipo : TYPE INT
            DESCRIPTION. Indicates the model to be used. 
            {1:AR_ASLG_HMM, 2:AR_MOG_HMM, 3:AR_MOG_RHMM, 4:LFS_AsHMM, 5:BMM, 6:DHMM , 7:FS_AsHMM, 9:LMSAR, 10:VAR_MOG_HMM, 11: KDE_AsHMM }
        Returns Nothing
        -------
        None.

        """
        modelos = {1:AR_ASLG_HMM, 2:AR_MOG_HMM, 3:AR_MOG_RHMM, 4:AsFS_AR_ASLG_HMM, 5:BMM, 6:DHMM , 7:FS_AsHMM, 8:FS_HMM, 9:LMSAR, 10:VAR_MOG_HMM, 11:KDE_AsHMM, 12: CT_DHMM}
        self.model = modelos[tipo] 
        
        