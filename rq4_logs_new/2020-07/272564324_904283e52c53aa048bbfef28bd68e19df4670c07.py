# -*- coding: utf-8 -*-
import numpy as np

class MeshTwoPlates():

    def __init__(self, deck):
        self.deck = deck
        self.set_mesh_grid() 
     
    def set_mesh_grid(self):
        self.nx = int(self.deck.doc["Simulation"]["Number of Elements X"])
        self.ny = int(self.deck.doc["Simulation"]["Number of Elements Y"])
        self.q=float(self.deck.doc["Processing Parameters"]["Power Density"])
        self.Lx=float(self.deck.doc["Simulation"]["lenX"])
        self.Ly=float(self.deck.doc["Simulation"]["lenY"])
        self.dx=self.Lx/self.nx
        self.dy=self.Ly/self.ny
        X, Y = np.meshgrid(np.arange(0, self.ny), np.arange(0, self.nx))
        X=X[1,:].copy()
        Y=Y[:,1].copy()
        self.X = X
        self.Y = Y

        self.nx1, self.nx2 = self.nx, self.nx
        self.ny1, self.ny2 = int(self.ny/2), self.ny

  

# -------------- BEGIN DEFINING TEMPERATURE INITIAL CONDITION ----------
        T = np.zeros((self.ny, self.nx))        
        T[0:self.ny1, 0:self.nx1] = self.deck.doc["Materials"]["Material1"]["Domain Initial Temperature"] # Set array size and set the interior value with Tini
        T[self.ny1:self.ny2, 0:self.nx2] = self.deck.doc["Materials"]["Material2"]["Domain Initial Temperature"] # Set array size and set the interior value with Tini
        T[int(self.ny/2), 1:-1] = self.deck.doc["Processing Parameters"]["Temperature"]
        T[int(self.ny/2-1), 1:-1] = self.deck.doc["Processing Parameters"]["Temperature"]
        self.T = T.copy()
        self.T0=T.copy()
# -------------- END DEFINING TEMPERATURE INITIAL CONDITION ----------
             
# ------------ BEGIN THERMAL CONDUCTIVITY INITIAL CONDITION ----------        
        KtotalX= np.zeros((self.ny, self.nx)) 
        KtotalX[0:self.ny1, 0:self.nx1] = self.deck.doc["Materials"]["Material1"]["Thermal Conductivity X"]
        KtotalX[self.ny1:self.ny2, 0:self.nx2] = self.deck.doc["Materials"]["Material2"]["Thermal Conductivity X"]
        self.KtotalX=KtotalX
                                                                                         
        KtotalY= np.zeros((self.ny, self.nx)) 
        KtotalY[0:self.ny1, 0:self.nx1] = self.deck.doc["Materials"]["Material1"]["Thermal Conductivity Y"]
        KtotalY[self.ny1:self.ny2, 0:self.nx2] = self.deck.doc["Materials"]["Material2"]["Thermal Conductivity Y"]                                                                                       
        self.KtotalY=KtotalY         
# -------------- END THERMAL CONDUCTIVITY INITIAL CONDITION ----------
        
# ----------------- BEGIN DENSITY INITIAL CONDITION ------------------ 
        RhoTotal= np.zeros((self.ny, self.nx)) 
        RhoTotal[0:self.ny1, 0:self.nx1] = self.deck.doc["Materials"]["Material1"]["Density"]
        RhoTotal[self.ny1:self.ny2, 0:self.nx2] = self.deck.doc["Materials"]["Material2"]["Density"]                                                                                       
        self.RhoTotal=RhoTotal  
# ----------------- END DENSITY INITIAL CONDITION --------------------    
        
# ------------- BEGIN HEAT CAPACITY INITIAL CONDITION ---------------- 
        CpTotal= np.zeros((self.ny, self.nx)) 
        CpTotal[0:self.ny1, 0:self.nx1] = self.deck.doc["Materials"]["Material1"]["Cp"]
        CpTotal[self.ny1:self.ny2, 0:self.nx2] = self.deck.doc["Materials"]["Material2"]["Cp"]                                                                                       
        self.CpTotal=CpTotal  
# -------------- END HEAT CAPACITY INITIAL CONDITION -----------------
        
# -------------- BEGIN DIFFUSIVITY INITIAL CONDITION -----------------                                                                                    
        DiffTotalX = np.zeros((self.ny, self.nx)) 
        DiffTotalX[0:,0:]=self.KtotalX[0:,0:]/(self.RhoTotal[0:,0:]*self.CpTotal[0:,0:])
        self.DiffTotalX = DiffTotalX.copy()
        
        DiffTotalY = np.zeros((self.ny, self.nx)) 
        DiffTotalY[0:,0:]=self.KtotalY[0:,0:]/(self.RhoTotal[0:,0:]*self.CpTotal[0:,0:])
        self.DiffTotalY = DiffTotalY.copy()
# --------------- END DIFFUSIVITY INITIAL CONDITION ------------------ 

# --------------- BEGIN VISCOSITY INITIAL CONDITION ------------------         
        Visc=np.zeros((self.ny, self.nx))
        Visc[0:, 0:]=1.14*10**(-12)*np.exp(26300/T[0:, 0:])
        self.Visc=Visc.copy()
# --------------- END VISCOSITY INITIAL CONDITION -------------------- 

# ----BEGIN DEGREE OF INTIMATE CONTACT VISCOSITY INITIAL CONDITION----
        Dic=np.ones((self.ny, self.nx))
        self.dic=1/(1+0.45)
        Dic[self.ny1-1:self.ny1+1,1:-1]=self.dic
        self.Dic0=Dic.copy()
        self.Dic=Dic.copy()   
# ---- END DEGREE OF INTIMATE CONTACT VISCOSITY INITIAL CONDITION----  
        
# ----------------- BEGIN HEAT INPUT NITIAL CONDITION----------------           
        Q=np.zeros((self.ny, self.nx))
        # Q[int(self.ny/2), 0:] = self.q
        # Q[int(self.ny/2-1), 0:] = self.q
        self.Q=Q.copy()