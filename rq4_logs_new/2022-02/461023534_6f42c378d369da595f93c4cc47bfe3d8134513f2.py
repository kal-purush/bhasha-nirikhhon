import numpy as np
import pandas as pd
from ipumspy import IpumsApiClient, UsaExtract, readers, ddi
import ipumspy
from config import *
from pipeline import *
import weighted
from pathlib import Path
import time

ipums = IpumsApiClient(API_KEY)
DOWNLOAD_DIR = Path('downloads')

extract = UsaExtract(
    ["us2019a"],
    ["HHINCOME", "CIHISPEED", "VEHICLES", "UHRSWORK", "STATEFIP", "PUMA"],
)
ipums.submit_extract(extract)
print(f"Extract submitted with id {extract.extract_id}")

# wait for the extract to finish
ipums.wait_for_extract(extract)

# Download the extract
ipums.download_extract(extract, download_dir=DOWNLOAD_DIR)

# Get the DDI
ddi_file = list(DOWNLOAD_DIR.glob("*.xml"))[0]
ddi = readers.read_ipums_ddi(ddi_file)

# Get the data
ipums_df = readers.read_microdata(ddi, DOWNLOAD_DIR / ddi.file_description.filename)

float(00)

ipums_df[ipums_df['CIHISPEED'] != 0]['CIHISPEED'].min()

# start_time = time.time()
pulled = data_by_zip(['02835', '79901','75204', '90210'],ipums_df, {"HHINCOME": {"null": 9999999, "type": 'household'},
                                                    "UHRSWORK": {"null": 0, "type": 'individual'},
                                                    "VEHICLES": {"null": 0, "type": 'household'},
                                                    "CIHISPEED": {"null": 0, "type": 'individual'}})

pulled

for i in {"HHINCOME": {"null_val": 9999999, "type": 'household'},"HHINCOME2": {"null_val": 9999999, "type": 'household'}}:
    print(i)

pulled

pulled['02835']['HHINCOME']

pulled = data_by_zip(['02835', '79901','75204', '90210'],ipums_df,'HHINCOdM',9999999,'household')
print(time.time() - start_time)

qsdfs = data_by_zip(['02835', '79901','75204', '90210'],ipums_df,'HHINCOdM',9999999,'household')
qsdfs




ddi_codebook.get_variable_info('HHINCOME')

ipums_df['BEDROOMS'].median()

ipums_df = readers.read_microdata(ddi_codebook, [data file path])

x = data_by_zip('02906','acs_data/usa_00013.csv','HHINCOME',9999999,'household')
grouped = ipums_df[ipums_df['PERNUM'] == 1][['HHWT','HHINCOME','PUMA']].groupby('PUMA')
weighted.median(grouped['HHINCOME'], grouped['HHWT'])
grouped['HHINCOME']