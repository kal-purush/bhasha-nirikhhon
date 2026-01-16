# -*- coding: utf-8 -*-
"""
Created on Wed Jun 10 15:00:34 2020

@author: andre
"""

import matplotlib.pyplot as plt
from PIL import Image
import glob

class PlotsTwoPlates():
    
    def __init__(self, deck,meshes,T,Dic):
        self.deck = deck
        self.nsteps =  int(self.deck.doc["Simulation"]["Number Time Steps"])
        self.nsetepinterval =int(self.deck.doc["Plot"]["plot interval"])
        self.meshes=meshes
        self.T = T
        self.Dic=Dic
        self.set_plots()


# -------------- BEGIN NEW PLOT GENERATION---------- 
    def set_plots(self): 
    
        self.mfig=[]
        for i in range (0,self.nsteps,self.nsetepinterval):
            self.mfig.append(i)
        self.fignum = 0
        self.fig = plt.figure()
# -------------- END NEW PLOT GENERATION----------  

       
    def update_T(self,T):
        self.T = T
        
    def update_Dic(self,Dic):
        self.Dic = Dic
 
# -------------- BEGIN PLOTTING----------  
    def do_plots(self,m):
                plt.clf()
                self.fignum += 1
                print(m, self.fignum)
                # plt.figure( figsize=(8, 6), dpi=280)
                plt.gcf().set_size_inches(16, 8)
                plt.gcf().set_dpi(80)
                plt.pcolormesh(self.meshes.Y*self.meshes.dx, self.meshes.X*self.meshes.dy, self.T,vmin=float(self.deck.doc["Materials"]["Material1"]["Domain Initial Temperature"]), vmax=float(self.deck.doc["Processing Parameters"]["Temperature"]),cmap=self.deck.doc["Plot"]["Color Map"])
                # plt.pcolormesh(self.meshes.Y*self.meshes.dx, self.meshes.X*self.meshes.dy, self.Dic,vmin=self.meshes.dic, vmax=1,cmap=self.deck.doc["Plot"]["Color Map"])
                plt.colorbar()
                self.fig.suptitle('time: {:.2f}'.format( m*float(self.deck.doc["Simulation"]["Time Step"])), fontsize=16)
                plt.savefig(self.deck.plot_dirTemp+self.deck.doc["Plot"]["figure temperature name"]+ str("%03d" %self.fignum) + '.jpg')

                plt.clf()
                print(m, self.fignum)
                # plt.figure( figsize=(8, 6), dpi=280)
                plt.gcf().set_size_inches(16, 8)
                plt.gcf().set_dpi(80)
                # plt.pcolormesh(self.meshes.Y*self.meshes.dx, self.meshes.X*self.meshes.dy, self.T,vmin=float(self.deck.doc["Materials"]["Material1"]["Domain Initial Temperature"]), vmax=float(self.deck.doc["Processing Parameters"]["Temperature"]),cmap=self.deck.doc["Plot"]["Color Map"])
                plt.pcolormesh(self.meshes.Y*self.meshes.dx, self.meshes.X*self.meshes.dy, self.Dic,vmin=self.meshes.dic, vmax=1,cmap=self.deck.doc["Plot"]["Color Map"])
                plt.colorbar()
                self.fig.suptitle('time: {:.2f}'.format( m*float(self.deck.doc["Simulation"]["Time Step"])), fontsize=16)
                plt.savefig(self.deck.plot_dirDic+self.deck.doc["Plot"]["figure dic name"]+ str("%03d" %self.fignum) + '.jpg')

# -------------- END PLOTTING----------                        
                
                
                
                
                
                
                
                
        
    def do_animation(self):   
        frames = []
        # imgs = glob.glob("./output/*.jpg")
        imgs = glob.glob(self.deck.plot_dirTemp + '*.jpg')
        for i in imgs:
            new_frame = Image.open(i)
            frames.append(new_frame)
            print(i)
            
        direction=(self.deck.plot_dirTemp+self.deck.doc["Animation"]["name"]+'.gif')
        frames[0].save(direction, format='GIF',
                        append_images=frames[1:],
                        save_all=True,
                        duration=400, loop=0)       
    
    
        frames = []
        # imgs = glob.glob("./output/*.jpg")
        imgs = glob.glob(self.deck.plot_dirDic + '*.jpg')
        for i in imgs:
            new_frame = Image.open(i)
            frames.append(new_frame)
            print(i)
            
        direction=(self.deck.plot_dirDic+self.deck.doc["Animation"]["name"]+'.gif')
        frames[0].save(direction, format='GIF',
                        append_images=frames[1:],
                        save_all=True,
                        duration=400, loop=0)      
        
        
        
        
# plt.plot(Dic1[:,0],Dic1[:,1],Dic2[:,0],Dic2[:,1], Dic3[:,0], Dic3[:,1])
# plt.legend(['1.0MPa', '0.5MPa','0.1MPa']) 
# plt.xlabel('time (s)')
# plt.ylabel('Dic')