


from smoderp2d.src.main_classes.General    import Globals as Gl
import smoderp2d.src.processes.rainfall        as rain_f
import smoderp2d.src.processes.infiltration    as infilt
import numpy as np


## 
#   vrati matici s pozicemi elementu,
#   matici s pozicemi 
#   
def init_getIJel():
  import smoderp2d.src.flow_algorithm.D8 as D8_
  inflows = D8_.new_inflows(Gl.mat_fd)
  
  
  getEl = np.zeros([Gl.r,Gl.c],int)
  getIJ = []
  getElIN = []
  
  
  el = int(-1)
  # prvni cyklus priradi 
  # k i j bunky jeji poradi ve vektoru
  # a k elementu vektoru i j bunky
  for i in Gl.rr:
    for j in Gl.rc[i]:  
      el += int(1)
      getIJ.append([i,j])
      getEl[i][j] = el

  
      
      
  # druhy cyklus priradi 
  # k elementu list elementu v inflows
  for i in Gl.rr:
    for j in Gl.rc[i]:  
      n = len(inflows[i][j])
      tmp = [-int(99)]*n
      getElIN.append(tmp)
      
      for z in range(n):
        ax = inflows[i][j][z][0]
        bx = inflows[i][j][z][1]
        iax = i+ax
        jbx = j+bx
        getElIN[z] = getEl[iax][jbx]
        
  return el, getEl, getElIN, getIJ
  

class ImplicitSolver:
  
  
  def __init__(self):
    
    self.A = np.zeros([Gl.r,Gl.c],float)
    self.b = np.zeros([Gl.r,Gl.c],float)
    self.hnew = np.ones([Gl.r,Gl.c],float)
    self.hold = np.zeros([Gl.r,Gl.c],float)
    
    # interval srazky
    self.tz = 0 
    self.total_time = 0
    
    self.nEl, \
    self.IJtoEl_a, \
    self.ELinEL_l, \
    self.ELtoIJ = init_getIJel() 
    
 
  
  def fillAmat(self):
    
    PS, self.tz = rain_f.timestepRainfall(Gl.itera,self.total_time,Gl.delta_t,self.tz,Gl.sr)
    
    
    for i in range(self.nEl):
      NS, sum_interception, rain_arr.arr[i][j].veg_true = rain_f.current_rain(rain_arr.arr[i][j], rainfall, sum_interception)
      print i
    
    
  
  

 
 
 
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
  