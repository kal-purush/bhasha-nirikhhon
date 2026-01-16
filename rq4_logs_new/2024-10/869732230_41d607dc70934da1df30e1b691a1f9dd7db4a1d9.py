#%%
# import pandas as pd 
import os
import libreach as lr
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('qtagg')
# this file is in src/, we want the repo name which is parent
path_repo = os.path.dirname(os.path.dirname(__file__))
import tkinter as tk
from tkinter import filedialog

folder_selected = ""

def select_folder():
    root = tk.Tk()
    root.withdraw()  # Hide the main window
    folder_selected = filedialog.askdirectory(title="choose recordings directory:")  # Open the dialog to select a folder
    return folder_selected

# Use the function
path_datadir = select_folder()
if path_datadir:
    print(f"You selected: {path_datadir}")
else:
    print("No folder selected")

# the name of the folder containing dirname

# name of the R file
fname_coords = "rotation_matrix_2"
tracker_name = "HOLISTIC_OPENSIM"
path_coords = os.path.join(path_datadir, fname_coords,tracker_name, "xyz_"+tracker_name+"_labelled.csv")

df    = pd.read_csv(path_coords)
data = df[['left_foot_index_x', 'left_foot_index_y', 'left_foot_index_z']].to_numpy()
R,X0 = lr.get_rotmat_x0(data)

# %%

# fname_data = os.path.join(dirname, 'example_jacks.csv')

#%% in this cell i just want to create a plotting function we can make rapid changes to. 
import matplotlib.pyplot as plt
fname_data = "trial_9_reach" #%12 10 9 7 6 5
path_data = os.path.join(path_datadir, fname_data,tracker_name, "xyz_"+tracker_name+"_labelled.csv")
pddata = pd.read_csv(path_data)

def draw_body_parts(pddata,R,x0,indrange,pm = .1):
  f,ax = plt.subplots()
  # set axis as 3d
  ax = f.add_subplot(111, projection='3d')

  for ir in range(0,len(indrange)):
    
    # Colors!
    # set the plotting color to be some grey dependent on ir
    c = (ir/len(indrange),ir/len(indrange),ir/len(indrange))
    # set the plotting color cr to be a darker red, depending on ir
    cr = (1,ir/len(indrange),ir/len(indrange))
    # set the plotting color cb to be a darker blue, depending on ir
    cb = (ir/len(indrange),ir/len(indrange),1)


    # Torso!
    sh_l = R @ np.array([pddata['left_shoulder_x'][indrange[ir]],pddata['left_shoulder_y'][indrange[ir]],pddata['left_shoulder_z'][indrange[ir]]])
    sh_r = R @ np.array([pddata['right_shoulder_x'][indrange[ir]],pddata['right_shoulder_y'][indrange[ir]],pddata['right_shoulder_z'][indrange[ir]]])
    hi_r = R @ np.array([pddata['right_hip_x'][indrange[ir]],pddata['right_hip_y'][indrange[ir]],pddata['right_hip_z'][indrange[ir]]])
    hi_l = R @ np.array([pddata['left_hip_x'][indrange[ir]],pddata['left_hip_y'][indrange[ir]],pddata['left_hip_z'][indrange[ir]]])

    sh_lz = sh_l - x0# - sho_fix
    sh_rz = sh_r - x0# - sho_fix
    hi_rz = hi_r - x0# - sho_fix
    hi_lz = hi_l - x0# - sho_fix

    ax.plot([sh_lz[0],sh_rz[0],hi_rz[0],hi_lz[0],sh_lz[0]],
            [sh_lz[1],sh_rz[1],hi_rz[1],hi_lz[1],sh_lz[1]],
            [sh_lz[2],sh_rz[2],hi_rz[2],hi_lz[2],sh_lz[2]],c=c,linewidth=3)

    # as above, right arm
    el_r = R @ np.array([pddata['right_elbow_x'][indrange[ir]],pddata['right_elbow_y'][indrange[ir]],pddata['right_elbow_z'][indrange[ir]]])
    wr_r = R @ np.array([pddata['right_wrist_x'][indrange[ir]],pddata['right_wrist_y'][indrange[ir]],pddata['right_wrist_z'][indrange[ir]]])
    # ha_r = R @ np.array([pddata['right_hand_x'][indrange[ir]],pddata['right_hand_y'][indrange[ir]],pddata['right_hand_z'][indrange[ir]]])

    el_rz = el_r - x0
    wr_rz = wr_r - x0
    
    ax.plot([sh_rz[0],el_rz[0],wr_rz[0]],
            [sh_rz[1],el_rz[1],wr_rz[1]],
            [sh_rz[2],el_rz[2],wr_rz[2]],c=cr,linewidth=3)  

    # as above, left arm
    el_l = R @ np.array([pddata['left_elbow_x'][indrange[ir]],pddata['left_elbow_y'][indrange[ir]],pddata['left_elbow_z'][indrange[ir]]])
    wr_l = R @ np.array([pddata['left_wrist_x'][indrange[ir]],pddata['left_wrist_y'][indrange[ir]],pddata['left_wrist_z'][indrange[ir]]])
    # ha_l = R @ np.array([pddata['left_hand_x'][indrange[ir]],pddata['left_hand_y'][indrange[ir]],pddata['left_hand_z'][indrange[ir]]])

    el_lz = el_l - x0
    wr_lz = wr_l - x0 
    # ha_l = ha_l

    ax.plot([sh_lz[0],el_lz[0],wr_lz[0]],
            [sh_lz[1],el_lz[1],wr_lz[1]],
            [sh_lz[2],el_lz[2],wr_lz[2]],c=cb,linewidth=3)

    # as above, right leg
    kn_r = R @ np.array([pddata['right_knee_x'][indrange[ir]],pddata['right_knee_y'][indrange[ir]],pddata['right_knee_z'][indrange[ir]]])
    an_r = R @ np.array([pddata['right_ankle_x'][indrange[ir]],pddata['right_ankle_y'][indrange[ir]],pddata['right_ankle_z'][indrange[ir]]])
    ft_r = R @ np.array([pddata['right_foot_index_x'][indrange[ir]],pddata['right_foot_index_y'][indrange[ir]],pddata['right_foot_index_z'][indrange[ir]]])
                        
    kn_rz = kn_r - x0
    an_rz = an_r - x0
    ft_rz = ft_r - x0

    ax.plot([hi_rz[0],kn_rz[0],an_rz[0],ft_rz[0]],
            [hi_rz[1],kn_rz[1],an_rz[1],ft_rz[1]],
            [hi_rz[2],kn_rz[2],an_rz[2],ft_rz[2]],c=cr,linewidth=3)
    
    # as above, left leg
    kn_l = R @ np.array([pddata['left_knee_x'][indrange[ir]],pddata['left_knee_y'][indrange[ir]],pddata['left_knee_z'][indrange[ir]]])
    an_l = R @ np.array([pddata['left_ankle_x'][indrange[ir]],pddata['left_ankle_y'][indrange[ir]],pddata['left_ankle_z'][indrange[ir]]])
    ft_l = R @ np.array([pddata['left_foot_index_x'][indrange[ir]],pddata['left_foot_index_y'][indrange[ir]],pddata['left_foot_index_z'][indrange[ir]]])

    kn_lz = kn_l - x0
    an_lz = an_l - x0
    ft_lz = ft_l - x0

    ax.plot([hi_lz[0],kn_lz[0],an_lz[0],ft_lz[0]],
            [hi_lz[1],kn_lz[1],an_lz[1],ft_lz[1]],
            [hi_lz[2],kn_lz[2],an_lz[2],ft_lz[2]],c
            =cb,linewidth=3)

  # Plotting! axis limits and labels
  
  # set 3d axis limits to be sh_lz +/- pm
  ax.set_xlim3d([hi_lz[0]-pm,hi_rz[0]+pm])
  ax.set_ylim3d([hi_lz[1]-pm,hi_rz[1]+pm])
  ax.set_zlim3d([hi_lz[2]-pm,hi_rz[2]+pm])
  
  # label x axis
  ax.set_xlabel('X (m)')
  ax.set_ylabel('Y (m)')
  ax.set_zlabel('Z (m)')

  # set the default view to be aligned y axis
  ax.view_init(elev=80,azim=-90)
  plt.show(block=True)

  # return the right arm
  return [sh_rz,el_rz,wr_rz]

rightarm = draw_body_parts(pddata,R,X0,np.arange(0,pddata.shape[0],10),pm=.1)

# plot over time
f2,ax2 = plt.subplots()
bodypart = "wrist"
wr = pddata[["right_"+bodypart+"_x","right_"+bodypart+"_y","right_"+bodypart+"_z"]].to_numpy()
wr = wr @ R.T
wr = wr - X0
ax2.plot(pddata["sync_index"],wr[:,0],label="x")
ax2.plot(pddata["sync_index"],wr[:,1],label="y")
ax2.plot(pddata["sync_index"],wr[:,2],label="z")
ax2.set_xlabel("syncindex")
ax2.set_ylabel("Position (m?)")
ax2.legend()
plt.show(block=True)