import pandas as pd
import warnings
import sys
import os
import subprocess
import extractfile
import pipistack 
import hydrophobic 
import electrostatic 
import hydrogenbond 
import waterbridge 
import halogenbond 

file_path = sys.argv[1]
base_name = os.path.splitext(os.path.basename(file_path))[0]

df1, df3, df5, data_list, list_new_indices, list_new = extractfile.process_file(file_path)
print(f'Loading {file_path}')

df_pipistack = pipistack.pi_pi_interaction(df3, df1)[0]
print(f'Finish pi pi stacking')
df_hydrophobic = hydrophobic.hydrophobic_interaction(df3, df1)[0]
print(f'Finish hydrophobic interaction')
df_electrostatic = electrostatic.electrostatics_interaction(df3, df1)[0]
print(f'Finish electrostatic interaction')
df_hydrogenbond = hydrogenbond.hydrogen_bond_interaction(df3, df1)[0]
print(f'Finish hydrogen bond')
df_water = waterbridge.water_bridge_interaction(df3, df1, df5)[0]
print(f'Finish water bridge')
df_halogen = halogenbond.halogen_bond_interaction(df3, df1)[0]
print(f'Finish halogen bond')

df_summary = pd.concat([ df_pipistack, df_hydrophobic, df_electrostatic, df_hydrogenbond, df_water, df_halogen])
df_summary['Number'] = df_summary['Number'].astype('int64')
df_summary = df_summary.sort_values(by=['Ligand', 'Type', 'Number']).reset_index(drop=True)

print(df_summary)
df_summary.to_csv(f'{base_name}_protein_activesite.csv')



