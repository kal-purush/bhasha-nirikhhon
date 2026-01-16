# -*- coding: utf-8 -*-
"""
Created on Sun May  9 19:32:41 2021

@author: crdub
"""

import pandas as pd
import matplotlib.pyplot as plt

i=pd.read_csv('clean_aid.csv')

isr = i.query('country =="Israel"')

isr_list = isr.groupby(['fiscal_year'])
isr_total = isr_list['amt'].sum()
mil = isr_total.sum(level=['fiscal_year'])
mil = mil/1e9

# plot Israel aid by dac
fig,ax1 = plt.subplots(dpi=300)
fig.suptitle("Foreign Aid to Israel")
mil.plot(ax=ax1)
ax1.set_xlabel('Years')
ax1.set_ylabel('Million Dollars')
fig.tight_layout()
fig.savefig("irs_aid.png")

group_by_dac = isr.groupby(["dac_category"])
by_dac = group_by_dac['amt'].sum()
mil = by_dac.sum(level=['dac_category'])
mil = mil/1e6

print(mil)

fig,(ax1) = plt.subplots(dpi=300)
fig.suptitle("Top DA Categories, Millions of Dollars")
mil.plot.barh(ax=ax1,fontsize=7)
fig.tight_layout()
fig.savefig('ISRtop10.png')

#%%

afg = i.query('country=="Afghanistan"')

afg_list = afg.groupby(['dac_category'])
afg_total = afg_list['amt'].sum()
mil = afg_total.sum(level=['dac_category'])
mil = mil/1e9

# plot Israel aid by dac
fig,ax1 = plt.subplots(dpi=300)
fig.suptitle("Foreign Aid to Afghanistan")
mil.plot(ax=ax1)
ax1.set_xlabel('Years')
ax1.set_ylabel('Million Dollars')
fig.tight_layout()
fig.savefig("afg_aid.png")

group_by_dac = afg.groupby(["dac_category"])
by_dac = group_by_dac['amt'].sum()
mil = by_dac.sum(level=['dac_category'])
mil = mil/1e6

print(mil)

fig,(ax1) = plt.subplots(dpi=300)
fig.suptitle("Top DA Categories, Millions of Dollars")
mil.plot.barh(ax=ax1,fontsize=7)
fig.tight_layout()
fig.savefig('AFGtop10.png')

#%%

# where is other going?

oth = i.query('dac_category=="Other"')

con_list=oth.groupby(['country'])
con_total = con_list['amt'].sum()
mil = con_total.sum(level=['country'])
mil = mil/1e9

by_country = mil.sum(level='country') 
top_country = by_country.sort_values()
top_country = top_country[-10:]
print(top_country)

fig,(ax3)=plt.subplots(1,dpi=300)
top_country.plot.barh(ax=ax3, fontsize = 7)
ax3.set_xlabel('Top Other Recepient')
fig.tight_layout()
fig.savefig('TopOther.png')

#%%

isr = i.query('dac_category=="Other"')

isother_list = isr.groupby(['agency'])
iso_total = isother_list['amt'].sum()
mil = iso_total.sum(level=['agency'])
mil = mil/1e9

by_agency = mil.sum(level='agency') 
top_agency = by_agency.sort_values()
top_agency = top_agency[-10:]
print(top_agency)

fig,(ax3)=plt.subplots(1,dpi=300)
top_agency.plot.barh(ax=ax3, fontsize = 7)
ax3.set_xlabel('Top Other Agency')
fig.tight_layout()
fig.savefig('TopOtherAgency.png')

#%%

dod = i.query('agency =="DOD"')

con_list=dod.groupby(['country'])
con_total = con_list['amt'].sum()
mil = con_total.sum(level=['country'])
mil = mil/1e9
print(mil)

by_country = mil.sum(level='country') 
top_country = by_country.sort_values()
top_country = top_country[-10:]
print(top_country)

fig,(ax3)=plt.subplots(1,dpi=300)
top_country.plot.barh(ax=ax3, fontsize = 7)
ax3.set_xlabel('Top DOD Recepient')
fig.tight_layout()
fig.savefig('TopDOD.png')




