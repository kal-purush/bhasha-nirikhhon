# 1. Add ghost cells
# 2. Optimise calculation of lennard jones force, tail correction to make derivatives continuous.
# 3. choose gamma to be very small, check what is small gamma. Limit in which friction can be neglected. no clustering expected for small densities.
# 4. monitor minimization, energy and force graphs with time. total force should be equal to zero.
# 5. momentum conservation or energy conservation (KE + PE).
# 6. what is temperature?

# 7. centre of mass accelration numerically and theoretically
# 8. angular momentum of the system
# 9. fix N and Gamma, study velocity distribution and centre of mass motion
# 10. try to avoid overlap while initialising system, rij<=alpha*sigmaij

# 11. Change force profile and fix overflow problems
# 12. lower DELTA_T
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from random import uniform
import matplotlib.animation as animation
from copy import deepcopy
import time

directions = np.array([[1,0],[0,1],[1,1],[-1,1],[-1,-1],[1,-1],[-1,0],[0,-1],[0,0]])
class Agent:
    def __init__(self,ini_pos, ini_velocity,force_propul,angle,agentId,cell,potential = 0,force_others=0):
        self.position = ini_pos
        self.velocity = ini_velocity
        self.force = force_others
        self.force_propul = force_propul
        self.angle = angle
        self.potential = potential
        self.ID = agentId
        self.cell = cell

def findDirection(angle):
    return np.array([np.cos(angle), np.sin(angle)]).reshape(2,1)

def ljp(r,eps=1,sig=1):
    if(r<=0.2):
      r = 0.2
    return 4*eps*(sig**12/r**12 - sig**6/r**6)

def ljf(r,eps=1,sig=1):
    if(r<0.2):
      r = 0.2
    return 48*eps*(sig**12/r**14 - sig**6/(2*(r**8)))

def lennard_jones_potential(r,r_cutoff = 2.5,eps=1, sig=1):
    if(r>=r_cutoff):
      return 0
    return ljp(r,eps,sig)-ljp(r_cutoff,eps,sig)+(r-r_cutoff)*ljf(r_cutoff,eps,sig)
    # return ljp(r,eps,sig)-ljp(r_cutoff,eps,sig)

def lennard_jones_force(r,r_cutoff = 2.5,eps=1, sig=1):
    if(r>=r_cutoff):
      return 0
    return ljf(r,eps,sig)-ljf(r_cutoff,eps,sig)
    # return ljf(r,eps,sig)

def getValue(ix,iy,leny):
    return int(ix*(leny-1) + iy)

def ifNeighbour(agent1_pos, agent2_pos,agent1_ID,agent2_ID,Ral):
    r = np.sqrt(np.sum(np.square(agent1_pos-agent2_pos)))
    rel_r = np.asarray(agent1_pos-agent2_pos)
    if(r<=Ral):
        f = lennard_jones_force(r)*rel_r
        p = lennard_jones_potential(r)
        return f,p
    else:
        return np.array([0.0,0.0]).reshape((2,1)),0.0
    
def makeCellsGrid(agents,radius, occupants,Lx,Ly):
    x = np.arange(0, Lx+radius, radius)
    y = np.arange(0, Ly+radius, radius)
    lenx = len(x)
    leny = len(y)
    make_map = np.arange((lenx-1)*(leny-1))
    make_map = make_map.reshape((lenx-1),(leny-1))
    grid = np.zeros(((lenx-1),(leny-1),occupants))
    grid_next = np.zeros((lenx-1)*(leny-1))
    for i,a in enumerate(agents):
        index_x = a.position[0]//radius
        index_y = a.position[1]//radius
        val =  getValue(index_x,index_y,leny)
        a.cell = val
        grid[int(index_x)][int(index_y)][int(grid_next[val])] = a.ID
        grid_next[int(val)]+=1
    return grid.astype(int), make_map.astype(int)

def findNearest(agents, grid, map, max_occupants,Ral,Lx,Ly):
    # print(grid)
    neigh_listAl = np.zeros((len(agents)+1,max_occupants))
    last_x = grid.shape[0]
    last_y = grid.shape[1]
    checkForcesx = np.ones((N+1,N+1))*(-1)
    checkForcesy = np.ones((N+1,N+1))*(-1)
    _potential = np.ones((N+1,N+1))*(-1)
    for a in agents:
        flag2 = 0
        cell = a.cell
        ix = np.where(map==cell)[0][0]
        iy =  np.where(map==cell)[1][0]
        a.force = np.array([0.0,0.0]).reshape((2,1))
        a.potential = 0.0
        for d in directions:
            offset_x = 0
            offset_y = 0
            new_ix = ix+d[0]
            new_iy = iy+d[1]
            if(new_ix<0):
                new_ix=last_x-1
                offset_x = -1.0*Lx
            elif(new_ix>=grid.shape[0]):
                new_ix = 0
                offset_x = Lx*1.0
            if(new_iy<0):
                new_iy=last_y-1
                offset_y = -1.0*Ly
            elif(new_iy>=grid.shape[1]):
                new_iy = 0
                offset_y = Ly*1.0
            for i in range(max_occupants):
                if(grid[new_ix][new_iy][i]!=0): 
                    if(grid[new_ix][new_iy][i]==a.ID):
                        continue
                    if(checkForcesx[a.ID][agents[grid[new_ix][new_iy][i]-1].ID]!=-1):
                        f = np.array([checkForcesx[a.ID][agents[grid[new_ix][new_iy][i]-1].ID],checkForcesy[a.ID][agents[grid[new_ix][new_iy][i]-1].ID]]).reshape((2,1))
                        p = _potential[a.ID][agents[grid[new_ix][new_iy][i]-1].ID]
                    else:
                        f,p = ifNeighbour(a.position, agents[grid[new_ix][new_iy][i]-1].position+np.array([offset_x,offset_y]).reshape((2,1)),a.ID,agents[grid[new_ix][new_iy][i]-1].ID,Ral)
                        checkForcesx[a.ID][agents[grid[new_ix][new_iy][i]-1].ID] = f[0]
                        checkForcesy[a.ID][agents[grid[new_ix][new_iy][i]-1].ID] = f[1]
                        checkForcesx[agents[grid[new_ix][new_iy][i]-1].ID][a.ID] = -1*f[0]
                        checkForcesy[agents[grid[new_ix][new_iy][i]-1].ID][a.ID] = -1*f[1]
                        _potential[a.ID][agents[grid[new_ix][new_iy][i]-1].ID] = p
                        _potential[agents[grid[new_ix][new_iy][i]-1].ID][a.ID] = p
                    a.force+=f
                    a.potential+=p/2
#                     if(yes_neigh and flag2<max_occupants-1):
#                         neigh_listAl[a.ID][flag2] = grid[new_ix][new_iy][i]
#                         flag2+=1
                else:
                    break
    return checkForcesx,checkForcesy

def getNeighList(agents,max_occupants,Lx,Ly,Ral):
    grid, map = makeCellsGrid(agents,Ral,len(agents),Lx,Ly)
#     print(grid)
    checkForcesx,checkForcesy = findNearest(agents, grid, map,max_occupants,Ral,Lx, Ly)
    # print(grid)
#     print(neigh_listAl)
    return checkForcesx,checkForcesy

def calculate_force(agents,max_occupants,Lx,Ly,Ral):
    checkForcesx,checkForcesy = getNeighList(agents,max_occupants,Lx,Ly,Ral)
    return checkForcesx,checkForcesy

# def InitialiseParticles(N,Lx,Ly, velocity = 1):
#     agents = []
#     for i in range(N):
#         pos = np.array([uniform(0,Lx),uniform(0, Ly)]).reshape((2,1))
#         angle1 = uniform(0,2*np.pi)
#         direction1 = findDirection(angle1)
#         angle2 = uniform(0,2*np.pi)
#         direction2 = findDirection(angle2)
#         agents.append(Agent(pos,velocity*direction1,direction2*force_prop,angle2,i+1,0))
#     checkForcesx,checkForcesy = calculate_force(agents,maxnums,Lx,Ly,Ral)
#     return agents,checkForcesx,checkForcesy

def checkProx(agents,pos,thr):
    for a in agents:
        if(np.sqrt(np.sum(np.square(pos-a.position)))<thr):
            return False
    return True

# def InitialiseParticles(N,Lx,Ly,thr = 0.25, velocity = 1):
#     agents = []
#     nums = 0
#     while(len(agents)<N):
#         pos = np.array([uniform(thr/2,Lx-thr/2),uniform(thr/2, Ly-thr/2)]).reshape((2,1))
#         if(checkProx(agents,pos,thr)):
#             angle1 = uniform(0,2*np.pi)
#             direction1 = findDirection(angle1)
#             angle2 = uniform(0,2*np.pi)
#             direction2 = findDirection(angle2)
#             agents.append(Agent(pos,velocity*direction1,direction2*force_prop,angle2,nums+1,0))
#             nums+=1
#     checkForcesx,checkForcesy = calculate_force(agents,maxnums,Lx,Ly,Ral)
#     return agents,checkForcesx,checkForcesy

def InitialiseParticles(N,Lx,Ly, alpha=1,velocity = 1):
    cells = int(np.sqrt(N))+1
    # print(cells)
    r_l = alpha
    x_s = np.linspace(0,Lx-1,cells)
    y_s = np.linspace(0,Ly-1,cells)
    print(x_s,y_s)
    # print(x_s)
    # print(y_s)
    nums = 0
    agents = []
    for x in x_s:
      if(nums==N):
        break
      for y in y_s:
          pos = np.array([x,y]).reshape((2,1))
          angle1 = uniform(0,2*np.pi)
          direction1 = findDirection(angle1)
          angle2 = uniform(0,2*np.pi)
          direction2 = findDirection(angle2)
          agents.append(Agent(pos,velocity*direction1,direction2*force_prop,angle2,nums+1,0))
          nums+=1
          if(nums==N):
            break
    checkForcesx,checkForcesy = calculate_force(agents,maxnums,Lx,Ly,Ral)
    return agents,checkForcesx,checkForcesy

# should be vectorised
def angular_update(agents):
    for a in agents:
        noise = np.random.normal(0, 1, 1)
        # angle_new = np.random.uniform(0,2*np.pi)
        angle_new = a.angle+noise*np.sqrt(2*del_t)
        dir = findDirection(angle_new)
        a.force_propul = force_prop*dir
        a.angle = angle_new
    return

def energy_minimise(agents, Lx,Ly,threshold_force=1e-7, threshold_time=250):
    agentsPosition = []
    agentsVelocity = []
    agentsPotential = []
    agentsPropul = []
    for i in range(threshold_time):
        if(i%1000==0):
            if(len(track_force)>0):
                print(track_force[-1],i)
#             print(i)
        total_force = 0
        position = []
        velocity = []
        potential = []
        propul = []
        for a in agents:
#             print(np.sum(np.square(np.add(a.force,a.force_propul))))
            total_force+=(np.sum(np.square(np.add(a.force,a.force_propul))))
            # print(a.position)
            a.position = np.array(a.position)+np.array(c1*a.velocity)+np.array(c2*np.add(a.force,a.force_propul))
            # print(a.position)
            # print(a.position)
            a.position[0] = a.position[0]%Lx
            a.position[1] = a.position[1]%Ly
            # print(a.velocity)
            a.velocity = Tau*a.velocity+(1/gamma)*(1-Tau)*(np.add(a.force,a.force_propul))
            # if(np.sum(np.square(a.velocity))>4):
            #   a.velocity = 2*(a.velocity/np.sqrt(np.sum(np.square(a.velocity))))
            # print(a.velocity)
            # potential.append(a.potential)
            position.append(a.position)
            velocity.append(a.velocity)
            potential.append(a.potential)
            propul.append(a.force_propul)
        agentsPosition.append(position)
        agentsVelocity.append(velocity)
        agentsPotential.append(potential)
        agentsPropul.append(propul)
        cX,cY = calculate_force(agents,maxnums,Lx,Ly,Ral)
        track_force.append(np.sqrt(total_force/len(agents)))
        if(np.sqrt(total_force/len(agents))<threshold_force):
            print(np.sqrt(total_force/len(agents)))
            print("breaking at time step", i)
            break
        
    return np.asarray(agentsPosition),np.asarray(agentsVelocity), np.asarray(agentsPotential), np.asarray(agentsPropul),cX,cY

def update(agents, iterations,Lx,Ly,output_file,threshold_force=0.01, threshold_time=250, animation=True):
    aP = []
    aV = []
    aPot = []
    aProp = []
    for i in range(iterations):
        print("Angular Update Step: ", i)
        angular_update(agents)
        ap,av,apot,aprop,cX,cY = energy_minimise(agents,Lx,Ly,threshold_force,threshold_time)
        if(len(aP)==0):
          aP=ap
          aV=av
          aPot = apot
          aProp = aprop
        else:
          aP = np.vstack((aP,ap))
          aV = np.vstack((aV,av))
          aPot = np.vstack((aPot,apot))
          aProp = np.vstack((aProp,aprop))
    if(animation):
        make_animation(aP,aV,aProp,output_file, Lx,Ly, framesPerSecond=25)
    return aP,aV,aPot,aProp,cX,cY

def make_animation(agentsPositions,agentsVelocity,agentsProp,output_file, Lx, Ly, framesPerSecond=25):

# Settings
  prop_mag = np.sqrt(np.sum(np.square(agentsProp[0][0])))
  agentsProp = agentsProp/prop_mag
  video_file = output_file
  clear_frames = False    # Should it clear the figure between each frame?
  fps = framesPerSecond
  # Output video writer
  FFMpegWriter = animation.writers['ffmpeg']
  metadata = dict(title='Collective Motion', artist='Matplotlib', comment='Move')
  writer = FFMpegWriter(fps=fps, metadata=metadata)
  fig, ax = plt.subplots()
  plt.tick_params(
      axis='x',         
      which='both',     
      bottom=False,     
      top=False,         
      labelbottom=False)
  plt.title('Collective Motion')
  with writer.saving(fig, video_file, 100):
      for i in range(int(len(agentsPositions)/20)):
            ax.set_xlim(0,Lx)
            ax.set_ylim(0,Ly)
            ax.scatter(agentsPositions[i*20][:,0],agentsPositions[i*20][:,1],color='blue', label = 'Agents', edgecolors = 'black', s=50, zorder=1, alpha = 0.8)
            ax.quiver(agentsPositions[i*20][:,0],agentsPositions[i*20][:,1],agentsVelocity[i*20][:,0]*500, agentsVelocity[i*20][:,1]*500,  color='black', units='inches' , angles='xy', scale=10,width=0.015,headlength=3,headwidth=2,alpha=0.8)   
            ax.quiver(agentsPositions[i*20][:,0],agentsPositions[i*20][:,1],agentsProp[i*20][:,0]*2, agentsProp[i*20][:,1]*2,  color='red', units='inches' , angles='xy', scale=10,width=0.015,headlength=3,headwidth=2,alpha=0.8)   
            writer.grab_frame()
            ax.clear()
        
  plt.clf()
  
def make_animation_cm(agentsPositions,agentsVelocity,output_file, Lx, Ly, framesPerSecond=25):

# Settings
  # prop_mag = np.sqrt(np.sum(np.square(agentsProp[0][0])))
  # agentsProp = agentsProp/prop_mag
  video_file = output_file
  clear_frames = False    # Should it clear the figure between each frame?
  fps = framesPerSecond
  # Output video writer
  FFMpegWriter = animation.writers['ffmpeg']
  metadata = dict(title='Collective Motion', artist='Matplotlib', comment='Move')
  writer = FFMpegWriter(fps=fps, metadata=metadata)
  fig, ax = plt.subplots()
  plt.tick_params(
      axis='x',         
      which='both',     
      bottom=False,     
      top=False,         
      labelbottom=False)
  plt.title('Collective Motion')
  with writer.saving(fig, video_file, 100):
      for i in range(int(len(agentsPositions)/10)):
            ax.set_xlim(0,Lx)
            ax.set_ylim(0,Ly)
            ax.scatter(agentsPositions[10*i][0],agentsPositions[10*i][1],color='blue', label = 'Agents', edgecolors = 'black', s=50, zorder=1, alpha = 0.8)
            ax.quiver(agentsPositions[10*i][0],agentsPositions[10*i][1],agentsVelocity[10*i][0], agentsVelocity[10*i][1],  color='black', units='inches' , angles='xy', scale=10,width=0.015,headlength=3,headwidth=2,alpha=0.8)   
            writer.grab_frame()
            ax.clear()
        
  plt.clf()

def velocity_progress(aV,N,output_file, framesPerSecond=25):
  video_file = output_file
  clear_frames = False    # Should it clear the figure between each frame?
  fps = framesPerSecond
  # Output video writer
  FFMpegWriter = animation.writers['ffmpeg']
  metadata = dict(title='Collective Motion', artist='Matplotlib', comment='Move')
  writer = FFMpegWriter(fps=fps, metadata=metadata)
  fig, ax = plt.subplots()
  # plt.tick_params(
  #     axis='x',         
  #     which='both',     
  #     bottom=False,     
  #     top=False,         
  #     labelbottom=False)
  maxi = np.max(aV)
  plt.title('Collective Motion')
  with writer.saving(fig, video_file, 100):
      for i in range(int(len(aV)/20)):
            ax.set_xlim(0,np.max(maxi))
            ax.set_ylim(0,N)
            ax.hist(aV[i*20])
            writer.grab_frame()
            ax.clear()
        
  plt.clf()

def q(x,b):
    if(x<=b):
      return 1
    return 0

def array_q(x,b=0.3):
    return np.array([q(xi,b) for xi in x])

def calculate_overlap(aPos,shift):
    return np.mean(array_q(np.sqrt(np.sum(np.square(aPos[shift]-aPos[0]),axis=1))))

def msd(aPos,idx):
    return np.mean((np.sum(np.square(aPos[idx]-aPos[0]),axis=1)))

def calculate_order(aV,idx):
    ave = 0
    for i in range(aV.shape[1]):
        ave+=aV[idx][i]/np.sqrt(np.sum(np.square(aV[idx][i])))
    ave = np.sqrt(np.sum(np.square(ave)))
    ave = ave/aV.shape[1]
    return ave