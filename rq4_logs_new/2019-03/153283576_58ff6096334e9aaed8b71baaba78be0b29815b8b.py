#Matix construction for CNN
import numpy as np
import pandas as pd
import glob, os
import sys
from timeit import default_timer as timer
###NEED TO HANDLE SEPERATE CLUSTERS SO ADD COLUMN TO DATA FILES WITH LABEL
###see sourcepaths[16]&8
def progressBar(value, endvalue, bar_length=20):
    
    percent = float(value) / endvalue
    arrow = '-' * int(round(percent * bar_length) - 1) + '>'
    spaces = ' ' * (bar_length - len(arrow))
    
    sys.stdout.write("\rPercent: [{0}] {1}%".format(arrow + spaces, int(round(percent * 100))))


# Creates dataframe for file
def DF(path):
    axislabels = ["DM", "Time", "S/N", "Width","Label"]
    Tfile = open(path,'r')
    data = np.fromfile(Tfile,np.float32,-1) 
    c = data.reshape((-1,5))
    df = pd.DataFrame(c,columns=axislabels)
    Tfile.close()
    return df

#Import data
#array of file locations and chosing the file to inspect with path
source_paths = []
Folder = "training_2"
"""
#file number in source_paths to open
x = 0
"""
#filling source_paths from the idir
for file in glob.glob(os.getcwd() + f"\{Folder}\\*\\"+ "*.dat"):
    source_paths.append(file)
    
#make directory for matrix files to go to
try:
    os.mkdir(os.getcwd()+"\matrix_files\\Playground2\\")
    os.mkdir(os.getcwd()+"\matrix_files\\Playground2\\Burst\\")
    os.mkdir(os.getcwd()+"\matrix_files\\Playground2\\Noise\\")
   
except:
    pass

#import dedispersion plan
df_ddp = pd.read_csv("dd_plan.txt")
"""print(df_ddp)"""
#setup array for step limits
dd_DM = np.array(df_ddp["DM_stop"])
dd_step = np.array(df_ddp["DM_step"])


#Constructing DF of possible DM values from ddp
DM_start = df_ddp["DM_start"]
DM_stop = df_ddp["DM_stop"]
DM_step = df_ddp["DM_step"]

DM_poss = [0.0] 

for i in range(len(DM_stop)):
    DM_range=DM_stop[i]-DM_start[i]
    num=round(DM_range/DM_step[i])
       
    for j in range(int(num)):
        DM_poss.append(round(DM_poss[-1]+DM_step[i],3))
        
"""print(DM_poss)"""
 
possDF = pd.DataFrame(DM_poss,columns=["DM"])
"""print(DF.head)"""


timer_1 = [] #creating the first zero
timer_2 = [] #time downsample/pad
timer_3 = [] #dm downsample/pad
timer_4 = [] #save




y=100
n_s = [] ###testing dm dimensions
for x in range(0,y):    
    #reading in data files    
    progressBar(x,y)
    
    
    clusterDF = DF(source_paths[x])
    clusterDF = clusterDF.sort_values(by="DM")
    clusterDF = clusterDF.reset_index(drop=True)
    clusterDF.DM = clusterDF.DM.astype(float).round(3) #have to round fp error out of dm
    """print(clusterDF)"""
    #set up arrays from data
    DM = np.array(clusterDF["DM"])
    TIME = np.array(clusterDF["Time"])
    SN = np.array(clusterDF["S/N"])
    
    
    #find ranges of time and dm
    #already sorted by dm so use difference between first and last row
    """
    df_dmrange = pd.concat([df.head(1),df.tail(1)])
    dmrange = np.array(df_dmrange["DM"])
    print(dmrange)
    """
    dmrange = np.array([DM[0],DM[-1]])

    #getting dm range, n dimension value
    a=possDF.loc[possDF["DM"] == dmrange[0]].index.item()
    b=possDF.loc[possDF["DM"] == dmrange[1]].index.item()
    
    n=b-a
    
  
    #time range, m dimension 
    ###same as before
    t_step = 256*(pow(10,-6))
    
    d= round(np.amin(TIME)/t_step)
    c= round(np.amax(TIME)/t_step)
   
    m = c-d
    m=int(m)

    
    #matrix construction
    #zero matrix of required dimension
    ###+1 to handle the fact you need to count the first value istelf
    
    zero = np.zeros((n+1,m+1))
    
    #find position of data point
    
    
    #h = DM_poss[DM_poss == DM[:]]
    v = np.round(TIME/t_step) 
    v = v[:]-d 
    #print(v)
    start_1=timer()
    for l in range(len(DM)):
        #dm axis location
        
        u = possDF.loc[possDF["DM"] == DM[l]].index.item() - a
        
        #time axis location
        
        
        #filling position
        zero[u][int(v[l])]=SN[l]
    end_1 = timer()
    timer_1.append(end_1-start_1)

    
    ###need to downsample in time by some bin amount
    #pixels wanted in time axis
    
    r_Tpixels=100
    
    #new time downsampled matrix of wanted size
    zero2=np.zeros((n+1,r_Tpixels))
    
    #case of needing to pad
    if m<r_Tpixels:
        start_2 = timer()
        pad=(r_Tpixels-m)/2
        pad=round(pad) #amount to pad from the top (~equal on the bottom)
        for q in range(len(zero[0,:])):
            ##iterator over num of columns
            zero2[:,q+pad]=zero[:,q]
    #case of Time downsample 
        end_2 = timer()
        timer_2.append(end_2-start_2)
    
    else:
        start_4 = timer()
        for p in range(len(zero[:,:])):
            ##iterator over num of rows
            #downsample into r_pixels in time axis
            dummy=np.array_split(zero[p,:],r_Tpixels)
            dummy2=[]
            for j in range(len(dummy)):
                #take max from each pixel group to give value to new pixel
                dummy2.append(max(dummy[j]))
            #fill new matrix by rows
            zero2[p]=dummy2
        end_4 = timer()
        timer_4.append(end_4-start_4)
    
    ###need to downsample or pad DM dimensions or nothing if n=r_DMpixels
    #pixels wanted in dm axis
    start_3=timer()
    r_DMpixels = 70
    
    #new matrix of required dimensions
    zero3=np.zeros((r_DMpixels,r_Tpixels))
    #case of needing to pad
    
    if n<r_DMpixels:
        pad=(r_DMpixels-n)/2
        pad=round(pad) #amount to pad from the top (~equal on the bottom)
        for q in range(len(zero2[:,:])):
            ##iterator over num of rows
            zero3[q+pad,:]=zero2[q,:]
            
    #case of DM downsample      
    else:
        for i in range(len(zero2[0,:])):
            ##iterator over num of columns
            #downsample into r_pixels in time axis
            dummy=np.array_split(zero2[:,i],r_DMpixels)
            dummy2=[]
            for j in range(len(dummy)):
                dummy2.append(max(dummy[j]))
            zero3[:,i]=dummy2
    end_3= timer()
    timer_3.append(end_3-start_3)
    #name of matrix file
    
    c_id= clusterDF["Label"].values
    if len(np.unique(c_id)) == 2:
        c_id = str(1)
        
    else:
        c_id = str(0)
        
    n_s.append((len(zero3[:,:]),len(zero3[0,:]))) ###testing
    new_name = source_paths[x].split(f"{Folder}\\")[1].split("\\")[1].replace(".dat","_m_"+c_id)
    #np.savetxt(os.getcwd()+"\matrix_files\\Final\\"+new_name, zero3)

    
    #temporary for playground
    if c_id == "0":
        np.save(os.getcwd()+"\matrix_files\\Playground2\\Noise\\"+new_name, zero3)
    else:
        np.save(os.getcwd()+"\matrix_files\\Playground2\\Burst\\"+new_name, zero3)
    
print(np.unique(n_s)) ###testing

print(f"Timer 1, read in and form first matrix: {np.mean(timer_1)} s" )
print(f"Timer 2, Time DS and pad: {np.mean(timer_2)} s" )
print(f"Timer 3, DM DS and pad: {np.mean(timer_3)} s" )
print(f"Timer 4, write files: {np.mean(timer_4)} s" )
#print(h)