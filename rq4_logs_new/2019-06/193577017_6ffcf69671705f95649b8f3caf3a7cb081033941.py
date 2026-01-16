import os
import pandas as pd
import kmc_ml_util



label_string = "t,A,B,OO,OA+AO,OB+BO,AA,AB+BA,BB,OOO,OOA+AOO,OOB+BOO,AOA,AOB+BOA,BOB,OAO,OAA+AAO,OAB+BAO,AAA,AAB+BAA,BAB,OBO,OBA+ABO,OBB+BBO,ABA,ABB+BBA,BBB"
labels = label_string.split(',')
print(labels)

## generate k_run.csv
#indir = "/home/nricke/work/tafel/KMC/500x_rep/" #older dataset, only scans surface coverage (not rate constants)
#indir = "/home/nricke/work/tafel/KMC/LV/k_run" #surface and rate constant scan for LV model
indir = sys.argv[1] # allow user to input directory containing data for a batch of KMC jobs

df_list = []
#for infile in os.listdir(indir):
for path, subdirs, files in os.walk(indir):
    print(path, subdirs, files)
    for name in files:
        if name[:3] == "run" and name[-3:] == "csv":
            df_list.append(pd.read_csv(path+'/'+name, header=None, names=labels))

print(len(df_list))

df = pd.concat(df_list)
df.to_csv("k_run.csv")

## generate k_run_prep.csv
df['O'] = 1. - df.A - df.B
df["OA+AO"] = df["OA+AO"]/2.
df["OB+BO"] = df["OB+BO"]/2.
df["AB+BA"] = df["AB+BA"]/2.

B3_list = ["OOA+AOO", "AOA", "AOB+BOA", "OAB+BAO", "AAB+BAA", "BAB"]
for B3 in B3_list:
    B3_string = B3[:3]
    B3_left2 = kmc_ml_util.B2_convert(B3_string[0], B3_string[1])
    B3_right2 = kmc_ml_util.B2_convert(B3_string[1], B3_string[2])
    B3_middle = B3_string[1]
    # Add the terms: "OOA_est", "AOA_est", "AOB_est", "OAB_est", "AAB_est", "BAB_est"
    df[B3[:3]+"_est"] = df[B3_left2]*df[B3_right2]/(df[B3_middle])
df.to_csv("k_run_prep.csv")