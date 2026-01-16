import numpy as np

# Function for conventional geothermal energy
def geothermal_conventional_model(params):
    cooling_tower_electricity = 864  # megawatt hour that we assume is the yearly electricity consumption
    cooling_tower_number      = 7/303.3
    drilling_waste_per_metre  = 450 # kilogram (for open hole diameter of 8.5 in and assume factor 3 production line to total volume drilled)
    lifetime_electricity_generated = (params["installed_capacity"] *
                                     (params["capacity_factor"] *
                                     (1 - params["auxiliary_power"]) *
                                      params["lifetime"] * 8760000) -
                                     (cooling_tower_electricity * 1000 * cooling_tower_number * 30))  # kilowatt hour
    number_of_wells.           = (np.ceil(params["installed_capacity"] / 
                                  params["gross_power_per_well"])) # Total number of wells is rounded up
    total_metres_drilled       = (number_of_wells *
                                  params["average_depth_of_wells"]) # metres
    diesel_consumption         = (total_metres_drilled * 
                                  params["specific_diesel_consumption"]) # MJ of thermal energy as diesel burned
    steel_consumption          = (total_metres_drilled * 
                                  params["specific_steel_consumption"])  # kilogram
    cement_consumption         = (total_metres_drilled * 
                                  params["specific_cement_consumption"]) # kilogram
    water_cem_consumption      = (cement_consumption * (1/0.65))  # kilogram
    drilling_mud_consumption   = (total_metres_drilled *
                                  params["specific_drilling_mud_consumption"])  # cubic meter
    drilling_waste             = - (total_metres_drilled * drilling_waste_per_metre) # kilogram (minus because waste)
    total_collection_pipelines = (number_of_wells * params["collection_pipelines"])  # metres
       
    number_of_wells_per_kwh            = number_of_wells              / lifetime_electricity_generated
    total_metres_drilled_per_kwh       = total_metres_drilled         / lifetime_electricity_generated
    diesel_consumption_per_kwh         = diesel_consumption           / lifetime_electricity_generated
    steel_consumption_per_kwh          = steel_consumption            / lifetime_electricity_generated
    cement_consumption_per_kwh         = cement_consumption           / lifetime_electricity_generated
    water_cem_consumption_per_kwh      = water_cem_consumption        / lifetime_electricity_generated
    drilling_mud_consumption_per_kwh   = drilling_mud_consumption     / lifetime_electricity_generated
    drilling_waste_per_kwh             = drilling_waste               / lifetime_electricity_generated
    total_collection_pipelines_per_kwh = total_collection_pipelines   / lifetime_electricity_generated
    power_plant_per_kwh                = params["installed_capacity"] / lifetime_electricity_generated
    
    # Find activities
    wellhead, diesel, steel, cement, water, \
    drilling_mud, drill_wst, wells_closr, coll_pipe, \
    plant, diesel_stim, co2, electricity_prod, _ = lookup_geothermal()
    
    return [
            (wellhead,     electricity_prod, np.array(number_of_wells_per_kwh)),
            (diesel,       electricity_prod, np.array(diesel_consumption_per_kwh)),
            (steel,        electricity_prod, np.array(steel_consumption_per_kwh)),
            (cement,       electricity_prod, np.array(cement_consumption_per_kwh)),
            (water,        electricity_prod, np.array(water_cem_consumption_per_kwh)),
            (drilling_mud, electricity_prod, np.array(drilling_mud_consumption_per_kwh)),
            (drill_wst,    electricity_prod, np.array(drilling_waste_per_kwh)),
            (wells_closr,  electricity_prod, np.array(total_metres_drilled_per_kwh)), # well closure
            (coll_pipe,    electricity_prod, np.array(total_collection_pipelines_per_kwh)),
            (plant,        electricity_prod, np.array(power_plant_per_kwh)),
            (co2,          electricity_prod, np.array(params["co2_emissions"]))
        ] 