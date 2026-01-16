
# coding: utf-8

# In[1]:

demand=0.2 #m3/person-day

Electric_Utility="SFPUC"
Electric_Utility_pipes="2010 CA Power Mix"
pipe_material="PE"
miles_to_disposal=30
miles_transport=50
discount=0.04
lifetime_treatment=25
lifetime=50
#pop_density=500 #people/km2
street_density=8000 #m/km2
pump_operating_fraction=0.8
storage_days=3
tank_height=3
retention_time=6 #h
consumption_pressure=20 #m


Treatment_operational_energy_m3=2.5 #MJ/m3
Treatment_capital_energy_m3=0.5 #MJ/m3
Treatment_operational_GHG_m3=0.25  #kgCO2/m3
Treatment_capital_GHG_m3= 0.07  #kgCO2/m3


#specify model parameters
electricity_cost=0.12 #$/kwh
specific_weight_water=9.807 #KN/m3
water_density=1000 #kg/m3
gravity=9.8 #m/s2
excavation_cost=4.6 #$/m3
excavation_energy=153 #MJ/m3
excavation_GHG=12 #kgCO2/m3
motor_efficiency=0.95
transport_cost=0.13 #$/ton-mile
transport_energy=8.16 #MJ/ton-mile
transport_GHG=0.656 #kgCo2/ton-mile
steel_cost=0.769 #$/kg
steel_GHG=1.3 #kgCO2/kg
steel_energy=17.5
steel_sheet_mass=186.9
steel_sheet_area=3.72
cement_energy=2820 #MJ/m3
cement_GHG=330 #kgCO2/m3
cement_cost=0.00005 #$/kg
reinf_concrete_energy=19 #Mj/$
reinf_concrete_GHG=1.794 #kgCo2/$
reinf_concrete_cost=115 # $/m3
media_filtration_energy=0.05 #kWh/m3 (Opportunities and Economics of water reuse)
flocculation_energy=0.05 #kWh/m3 of tank volume (Lee,2010)

#specify treatment parameters
MBR_GHG=0.23
sludge_mass=0.1 #kg/m3 water treated
percent_fertilizer= 0.5 #percent of sludge disposed as fertilizer
percent_landfill= (1-percent_fertilizer) 
landfill_GHG=0.04
fertilizer_GHG=0
chlorine_energy=42 #MJ/kg
chlorine_GHG=0.74 #kg/kg
chlorine_mass=15 #mg/L
chlorine_retention_time=1 #h
Alum_density=120 #mg/L 
Alum_energy=0.91 #MJ/kg
Alum_GHG=0.07 #kgCO2/kg

RSF_rate=293 #m3/m2-day (Tchobanoglous small and decentralized)
RSF_sand_depth=0.6 #m (Tchobanoglous small and decentralized - deep bed sand) 

RSF_anthracite_depth=0.75
RSF_sand_density=1602 #kg/m3
RSF_sand_energy=0.147 #MJ/kg
RSF_sand_GHG=0.0104 #kgCO2/kg
RSF_anthracite_density=1100 #kg/m3
RSF_anthracite_energy=0.231 #MJ/kg
RSF_anthracite_GHG=0.06 #kgCO2/kg
flocculation_time=1 #hours
coagulation_time=0.3 #hours

Grinder_pump_usage=0.2 # h/day
UV_rating=9.5 #W/m3/d
UV_capital_cost=4 #$/W 2014
UV_usage=12 #h/day
UV_lifetime= 5 #y
Screen_Filter_capital_cost=2500 #($ 2015)
Screen_Filter_capital_energy=23800
Screen_Filter_capital_GHG=1632 
Grinder_pump_hp=1.5 
Grit_chamber_time=0.02 #h (small and decentralized Tchobanoglous)
filter_screen_energy=0.008 #kWh/m3 (Opportunities and Economics of water reuse)


# In[ ]:


