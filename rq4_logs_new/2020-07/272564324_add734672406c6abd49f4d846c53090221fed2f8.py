# -*- coding: utf-8 -*-
import numpy as np


class SolvesTwoPlates:
    
    def __init__(self, deck,model_HT,meshes,plots,model_IC):
        self.deck = deck
        # Compute the TEMPERATURE for each node
        self.model_HT = model_HT
        self.model_IC=model_IC
        self.meshes=meshes  
        self.plots = plots
        self.do_solver()
        
            
        
        
        
    def do_solver(self):
        for m in range(int(self.deck.doc["Simulation"]["Number Time Steps"])):
# -------------- CALCULATE TEMPERATURE FOR EACH STEP INCREMENT----------             
            self.meshes.T0, self.meshes.T = self.model_HT.do_timestep(self.meshes.T0, self.meshes.T, self.meshes.DiffTotalX, self.meshes.DiffTotalY, self.meshes.Q)
# -------------- FORCE TEMPERATURE AT THE INTERFACE: ISOTHERMAL CONDITION----------             
            self.meshes.T[int(self.meshes.ny/2), 1:-1] = self.deck.doc["Processing Parameters"]["Temperature"]
            self.meshes.T[int(self.meshes.ny/2-1), 1:-1] = self.deck.doc["Processing Parameters"]["Temperature"]
# -------------- UPDATE T0----------             
            self.meshes.T0=self.meshes.T.copy()
# -------------- CALCULATE VISCOSITY FOR EACH STEP INCREMENT----------             
            self.meshes.Visc=self.model_IC.viscosity_timestep(self.meshes.Visc, self.meshes.T)
# -------------- CALCULATE Dic FOR EACH STEP INCREMENT----------             
            self.meshes.Dic=self.model_IC.dic_timestep(self.meshes.Dic, self.meshes.Dic0, self.meshes.Visc, float(self.deck.doc["Simulation"]["Time Step"]))
# -------------- UPDATE THE INTEGRAL----------             
            self.model_IC.aux=self.model_IC.update_aux( float(self.deck.doc["Simulation"]["Time Step"]), self.meshes.Visc)
# -------------- UPDATE Dic----------             
            self.meshes.Dic=np.clip(self.meshes.Dic,0,1)
# -------------- DO PLOT ACCORDING TO THE SELECTED INTERVAL----------             
            if m in self.plots.mfig:
                self.plots.update_Dic(self.meshes.Dic)
                self.plots.update_T(self.meshes.T)       
                self.plots.do_plots(m)    
        self.plots.do_animation()
        self.model_IC.calculate_average_dic()
            
     