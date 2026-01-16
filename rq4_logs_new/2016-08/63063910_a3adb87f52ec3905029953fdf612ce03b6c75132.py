# -*- coding: utf-8 -*-
"""
Created on Tue Aug 23 12:22:28 2016
Topographic view of coherence pairs
@author: vavra
"""

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
import scipy.io as sio


plt.close('all')

#Import matlab files
data_mat = sio.loadmat('D:\Filip_PSI_mysi\Coherence_StatisticFinalTables\Koherence_zobrazeni_data\PSI_dif_p.mat')
coord_mat = sio.loadmat('Locations.mat')
brain = mpimg.imread('brain2.png')


x_cord = coord_mat['x_pair_sel']       #x_pair_all for all pairs
y_cord = coord_mat['y_pair_sel']
index = coord_mat['index'][0]           #selected pair indices

dif_treat = data_mat['dif_treat']
dif_treat = dif_treat[:,::3]                #[:,::2] selects 20-30
#dif_psi_treat = data_mat['dif_psi_treat']
p_treat = data_mat['p_treat']                #[index]        #select 36 from 66 pairs
p_treat = p_treat[:,::3]                    #[:,::2] selects 20-30
#p_psi_treat = data_mat['p_psi_treat'][index]

no_el = len(index)

#------------Treatment

drug = ['PSI ','.jpg']
band_title = ['Delta:1.0-4.0 Hz', 'Theta: 4.0 - 8.0 Hz', 'Alpha: 8.0 - 12.0 Hz', 'Beta: 12.0 - 25.0 Hz', 'High Beta: 25.0 - 30.0 Hz','Gamma: 30.0 - 40.0 Hz']
band_name = ['Delta','Theta','Alpha','Beta','High_Beta','Gamma']
for i in np.arange(p_treat.shape[1]):
    plt.figure(i)
    plt.imshow(brain)
    plt.tick_params(labeltop=False, labelbottom=False, bottom=False, top=False
                    ,left=False,right=False, labelright=False,labelleft=False)
    plt.plot([400,450],[1500,1500],linewidth = 12,color = 'blue')
    plt.plot([550,600],[1500,1500],linewidth = 12,color = 'red')      
    plt.plot([600,700],[1500,1500],linewidth = 6,color = 'red')
    plt.plot([300,400],[1500,1500],linewidth = 6,color = 'blue')  
    plt.plot([700,800],[1500,1500],linewidth = 2,color = 'red')
    plt.plot([200,300],[1500,1500],linewidth = 2,color = 'blue')
    plt.text(400,1450,'-75%',fontsize=6)
    plt.text(300,1450,'-50%',fontsize=6)
    plt.text(200,1450,'-25%',fontsize=6)
    plt.text(550,1450,'75%',fontsize=6)
    plt.text(650,1450,'50%',fontsize=6)
    plt.text(750,1450,'25%',fontsize=6)
              
                    
    for j in np.arange(no_el):        
        if (dif_treat[j,i] > 0):
                color = 'red'
        else:
                color = 'blue'
                
        if (abs(dif_treat[j,i]) > 75):           
            linew = 12
            show = 1
        elif (abs(dif_treat[j,i]) > 50):           
            linew = 6
            show = 1
        elif (abs(dif_treat[j,i]) > 25):           
            linew = 2
            show = 1
        else:               #if relative change is lower then 25%, don't show
            show = 0
        
        if (p_treat[j,i] > 0.05):   #if relative change is not significant, don't show
            show = 0
                    
        if show:        
            plt.plot(x_cord[j],y_cord[j],color,linewidth = linew,alpha = 0.4)  

    #save the figure        
    plt.title(drug[0] + band_title[i])            
    f_name = drug[0]+band_name[i]+'.jpeg'
    plt.savefig(f_name, dpi=300, facecolor='w', edgecolor='w',
    orientation='portrait', papertype=None, format=None,
    transparent=False, bbox_inches=None, pad_inches=0.1,
    frameon=None)
plt.show

# ---------Psilocin Treatment      



#drug = ['PSI_WAY ','.jpg']
#band_title = ['Delta:1.0-4.0 Hz', 'Theta: 4.0 - 8.0 Hz', 'Alpha: 8.0 - 12.0 Hz', 'Beta: 12.0 - 25.0 Hz', 'High Beta: 25.0 - 30.0 Hz','Gamma: 30.0 - 40.0 Hz']
#band_name = ['Delta','Theta','Alpha','Beta','High_Beta','Gamma']
#for i in np.arange(p_treat.shape[1]):
#    plt.figure(i+6)
#    plt.imshow(brain)
#    plt.tick_params(labeltop=False, labelbottom=False, bottom=False, top=False
#                    ,left=False,right=False, labelright=False,labelleft=False) 
#    plt.plot([400,450],[1500,1500],linewidth = 12,color = 'blue')
#    plt.plot([550,600],[1500,1500],linewidth = 12,color = 'red')      
#    plt.plot([600,700],[1500,1500],linewidth = 6,color = 'red')
#    plt.plot([300,400],[1500,1500],linewidth = 6,color = 'blue')  
#    plt.plot([700,800],[1500,1500],linewidth = 2,color = 'red')
#    plt.plot([200,300],[1500,1500],linewidth = 2,color = 'blue')
#    plt.text(400,1450,'-75%',fontsize=6)
#    plt.text(300,1450,'-50%',fontsize=6)
#    plt.text(200,1450,'-25%',fontsize=6)
#    plt.text(550,1450,'75%',fontsize=6)
#    plt.text(650,1450,'50%',fontsize=6)
#    plt.text(750,1450,'25%',fontsize=6)      
#    
#    for j in np.arange(no_el):       
#        if (dif_psi_treat[j,i] > 0):
#                color = 'red'
#        else:
#                color = 'blue'                
#        
#        if (abs(dif_psi_treat[j,i]) > 75):           
#            linew = 12
#            show = 1
#        elif (abs(dif_psi_treat[j,i]) > 50):           
#            linew = 6
#            show = 1
#        elif (abs(dif_psi_treat[j,i]) > 25):           
#            linew = 2
#            show = 1
#        else:
#            show = 0
#        
#        if (p_psi_treat[j,i] > 0.05):
#            show = 0
#                 
#        if show:        
#            plt.plot(x_cord[j],y_cord[j],color,linewidth = linew,alpha = 0.4)  
#            
#        plt.title(drug[0] + band_title[i])            
#        f_name = drug[0]+band_name[i]+'.jpeg'
#        plt.savefig(f_name, dpi=300, facecolor='w', edgecolor='w',
#        orientation='portrait', papertype=None, format=None,
#        transparent=False, bbox_inches=None, pad_inches=0.1,
#        frameon=None)
#plt.show         





