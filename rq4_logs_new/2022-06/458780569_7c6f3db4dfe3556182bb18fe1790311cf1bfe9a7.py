import os
import numpy as np
import pandas as pd
import glob


###1p
# note: for _rand_ sider: for all the others: side2  -> sorry for that (only for 1p)
path = 'C:/Users/Gesine/Documents/ACx/Stats/'
csv_files = glob.glob(os.path.join(path, "*1p*12.csv")) # change from 2p to 3p
for i, file in enumerate(csv_files):
    name = file[(len(path)+7):-7] # for 09,12,14,17: 7; for VCx: 8
    locals()[name] = pd.read_csv(file, header=None, delimiter=r"\s+")

I = pd.melt(pd.DataFrame(np.arctan2(beta2_1, beta1_1)))
I.insert(0,'Parameter', np.repeat('L2/3',250000))
I = I[['Parameter','value']]
I.insert(0,'side', np.repeat('l',250000))
r = pd.melt(pd.DataFrame(np.arctan2(beta2_1+beta2_2, beta1_1+beta1_2)))
r.insert(0,'Parameter', np.repeat('L2/3',250000))
r = r[['Parameter','value']]
r.insert(0,'side', np.repeat('r',250000))
l = ([Intercept[0].mean(), Intercept[1].mean(), Intercept[3].mean(), Intercept[4].mean()]) # mean, median, lowerBound, UB for L23 left ...
r = ([side2[0].mean(), side2[1].mean(), side2[3].mean(), side2[4].mean()])
l_std = ([Intercept[0].std(), Intercept[1].std(), Intercept[3].std(), Intercept[4].std()]) # mean, median, lowerBound, UB for L23 left ...
r_std = ([side2[0].std(), side2[1].std(), side2[3].std(), side2[4].std()])



### 2/3p
path = 'C:/Users/Gesine/Documents/ACx/Stats/'
csv_files = glob.glob(os.path.join(path, "*2p*rand.csv")) # change from 2p to 3p
for i, file in enumerate(csv_files):
    name = file[(len(path)+7):-9] # for 09,12,14,17: 7; for VCx: 8
    locals()[name] = pd.read_csv(file, header=None, delimiter=r"\s+")

I = pd.melt(pd.DataFrame(np.arctan2(beta2_1, beta1_1)))
I.insert(0,'Parameter', np.repeat('L2/3',250000))
I = I[['Parameter','value']]
I.insert(0,'side', np.repeat('l',250000))
L4 = pd.melt(pd.DataFrame(np.arctan2(beta2_1+beta2_3, beta1_1+beta1_3)))
L4.insert(0,'Parameter', np.repeat('L4',250000))
L4 = L4[['Parameter','value']]
L4.insert(0,'side', np.repeat('l',250000))
L5 = pd.melt(pd.DataFrame(np.arctan2(beta2_1+beta2_4, beta1_1+beta1_4)))
L5.insert(0,'Parameter', np.repeat('L5',250000))
L5 = L5[['Parameter','value']]
L5.insert(0,'side', np.repeat('l',250000))
r = pd.melt(pd.DataFrame(np.arctan2(beta2_1+beta2_2, beta1_1+beta1_2)))
r.insert(0,'Parameter', np.repeat('L2/3',250000))
r = r[['Parameter','value']]
r.insert(0,'side', np.repeat('r',250000))
L4r = pd.melt(pd.DataFrame(np.arctan2(beta2_1+beta2_2+beta2_3, beta1_1+beta1_2+beta1_3)))
L4r.insert(0,'Parameter', np.repeat('L4',250000))
L4r = L4r[['Parameter','value']]
L4r.insert(0,'side', np.repeat('r',250000))
L5r = pd.melt(pd.DataFrame(np.arctan2(beta2_1+beta2_2+beta2_4, beta1_1+beta1_2+beta1_4)))
L5r.insert(0,'Parameter', np.repeat('L5',250000))
L5r = L5r[['Parameter','value']]
L5r.insert(0,'side', np.repeat('r',250000))

l23_l = ([Intercept[0].mean(), Intercept[1].mean(), Intercept[3].mean(), Intercept[4].mean()]) # mean, median, lowerBound, UB for L23 left ...
l23_r = ([sider[0].mean(), sider[1].mean(), sider[3].mean(), sider[4].mean()])
l4_l = ([layerL4[0].mean(), layerL4[1].mean(),layerL4[3].mean(), layerL4[4].mean()])
l4_r = ([siderlayerL4[0].mean(), siderlayerL4[1].mean(), siderlayerL4[3].mean(), siderlayerL4[4].mean()])
l5_l = ([layerL5[0].mean(), layerL5[1].mean(), layerL5[3].mean(), layerL5[4].mean()])
l5_r = ([siderlayerL5[0].mean(), siderlayerL5[1].mean(), siderlayerL5[3].mean(), siderlayerL5[4].mean()])

l23_l_std = ([Intercept[0].std(), Intercept[1].std(), Intercept[3].std(), Intercept[4].std()]) # mean, median, lowerBound, UB for L23 left ...
l23_r_std = ([sider[0].std(), sider[1].std(), sider[3].std(), sider[4].std()])
l4_l_std = ([layerL4[0].std(), layerL4[1].std(),layerL4[3].std(), layerL4[4].std()])
l4_r_std = ([siderlayerL4[0].std(), siderlayerL4[1].std(), siderlayerL4[3].std(), siderlayerL4[4].std()])
l5_l_std = ([layerL5[0].std(), layerL5[1].std(), layerL5[3].mean(), layerL5[4].std()])
l5_r_std = ([siderlayerL5[0].std(), siderlayerL5[1].std(), siderlayerL5[3].std(), siderlayerL5[4].std()])


