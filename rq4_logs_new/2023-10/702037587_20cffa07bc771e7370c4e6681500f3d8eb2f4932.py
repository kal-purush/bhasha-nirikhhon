import pandas as pd
import numpy as np
import os

#------------------------------------------------------------
#                          INPUT
#------------------------------------------------------------

input_folder = 'input_databases'
input_ext = '.csv'

files = ['plumes_2022-01-01_2023-01-01',
         'states_2022',
         'states_2022_cap']

output_folder = 'dataframes'
output_ext = '.csv'

#------------------------------------------------------------
#                    READING & FILTERING
#------------------------------------------------------------
print("\nStarting reading data")

df_blank = pd.DataFrame(columns=['Lat', 'Lon', 'Date', 'Emission'])

df_hotspots = pd.read_csv(os.path.join(input_folder, files[0] + input_ext), sep=',')

columns_to_keep = ['plume_latitude', 'plume_longitude', 'datetime', 'emission_auto']
df_hotspots = df_hotspots[columns_to_keep]
df_hotspots = df_hotspots.rename(columns={'datetime':'Date',               # Renaming columns to more descriptive names
                                          'plume_latitude':'Lat',
                                          'plume_longitude':'Lon',
                                          'emission_auto': 'Emission'})
df_hotspots['Date'] = pd.to_datetime(df_hotspots['Date'])
df_hotspots = df_hotspots[np.isfinite(df_hotspots.Emission)]
# df_hotspots = df_hotspots.set_index(['Date'])

column_names = ['State', 'Value']
df_states = [pd.read_csv(os.path.join(input_folder, files[1] + input_ext), sep=',', header=None, names=column_names),
             pd.read_csv(os.path.join(input_folder, files[2] + input_ext), sep=',', header=None, names=column_names)]
state_fips_mapping = {
    'AL': '01', 'AK': '02', 'AZ': '04', 'AR': '05', 'CA': '06',
    'CO': '08', 'CT': '09', 'DE': '10', 'DC': '11', 'FL': '12', 
    'HI': '15', 'ID': '16', 'IL': '17', 'IN': '18', 'IA': '19',
    'KS': '20', 'KY': '21', 'LA': '22', 'ME': '23', 'MD': '24',
    'MA': '25', 'MI': '26', 'MN': '27', 'MS': '28', 'MO': '29',
    'MT': '30', 'NE': '31', 'NV': '32', 'NH': '33', 'NJ': '34',
    'NM': '35', 'NY': '36', 'NC': '37', 'ND': '38', 'OH': '39',
    'OK': '40', 'OR': '41', 'PA': '42', 'RI': '44', 'SC': '45',
    'SD': '46', 'TN': '47', 'TX': '48', 'UT': '49', 'VT': '50',
    'VA': '51', 'WA': '53', 'WV': '54', 'WI': '55', 'WY': '56',
    'GA': '13', 'PR': '72', 'VI': '78'
}
def state_to_fips(state):
    return state_fips_mapping.get(state, None)
df_states[0]['State_FIPS'] = df_states[0]['State'].apply(state_to_fips)

print("Exporting data terminated")

#------------------------------------------------------------
#                     EXPORTING DATA
#------------------------------------------------------------

print("\nStarting exporting data")


if not os.path.exists(output_folder):
    os.makedirs(output_folder)

df_blank.to_csv(os.path.join(output_folder,'df_blank' + output_ext))
df_hotspots.to_csv(os.path.join(output_folder,'df_hotspots' + output_ext))
df_states[0].to_csv(os.path.join(output_folder,'df_states1' + output_ext))
df_states[1].to_csv(os.path.join(output_folder,'df_states2' + output_ext))

print("Exporting data terminated")