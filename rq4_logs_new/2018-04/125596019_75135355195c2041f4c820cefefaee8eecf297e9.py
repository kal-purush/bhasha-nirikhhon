"""
file name: Core_Model.py
    core simulation: energy balance, constraints, optimization
    
authors:
    Fan (original model)
    Ken (reorganized model)
    Mengyao (nuclear vs. renewables)

version: _my180406
    modified from _my180403 for preliminary analysis of nuclear vs. renewables
    notes from Fan on orignal model:
        spatial scope: US
        data: Matt Shaner's paper with reanalysis data and US demand_series
        generation: natural gas, wind, solar, nuclear
        energy storage: one generic (pre-determined round-trip efficiency)
        curtailment: yes (free)
        unmet demand: no
    definitions in this version:
        dispatch: "used" generation, or generation used to meet demand
        curtailment: "unused" generation, or amount generated but tossed, subject to variable cost
        if dispatchable: dispatch + curtailment (= total generation) <= capacity * capacity factor
        if non-dispatchable: dispatch + curtailment (= total generation) == capacity * capacity factor

updates:
    (a) for wind and solar, set dispatch + curtailment == capacity * capacity_factor
        (i) in previous versions, dispatch <= capacity * capacity_factor
            --> "double counting" of capacity factor
        (ii) underlying assumption: using data from Shaner et al., 2018 - can change to other sources
    (b) included curtailment of wind, solar, and nuclear as decision variables 
    (c) added flag (flex_nuc_flag) for choosing constraints for nuclear depending on whether nuclear is assumed to be flexible or fixed
    
to-dos:
    confirm units of costs, capacity, charge/discharge, etc. - see notes
    
"""

#%% import modules

import cvxpy as cvx
import time
import datetime
import numpy as np

#%% loop to step through assumption list for each case

def core_model_loop(
        time_series,
        assumption_list,
        verbose
        ):
    num_cases = len(assumption_list)
    result_list = [dict() for x in range(num_cases)]
    for case_index in range(num_cases):
        if verbose:
            today = datetime.datetime.now()
            print today
            print assumption_list[case_index]
        result_list[case_index] = core_model(
            time_series, 
            assumption_list[case_index],
            verbose
            )                                            
    return result_list

#%% core energy balance and optimization model

def core_model(
        time_series, 
        assumptions,
        verbose
        ):

    # input data: demand, solar and wind capacity factors, flags    
    demand_series = time_series['demand_series'].copy()     # assumed to be normalized to 1 kW mean
    solar_series = time_series['solar_series']      # assumed to be normalized per kW capacity
    wind_series = time_series['wind_series']        # assumed to be normalized per kW capacity
    demand_flag = assumptions['demand_flag']        # if < 0, use demand series, else set to value
    if demand_flag >= 0:
        demand_series.fill(demand_flag)
    flex_nuc_flag = assumptions['flex_nuc_flag']    # if = 1 (True), nuclear is fully flexible / rampable; if = 0 (False), nuclear is fixed at full capacity
    
    # fixed costs assumed to be $ per time period (1 hour)
    fix_cost_natgas = assumptions['fix_cost_natgas']
    fix_cost_solar = assumptions['fix_cost_solar']
    fix_cost_wind = assumptions['fix_cost_wind']
    fix_cost_nuclear = assumptions['fix_cost_nuclear']
    fix_cost_storage = assumptions['fix_cost_storage']

    # variable costs assumed to be $ per kWh
    var_cost_natgas = assumptions['var_cost_natgas']
    var_cost_solar = assumptions['var_cost_solar']
    var_cost_wind = assumptions['var_cost_wind']
    var_cost_nuclear = assumptions['var_cost_nuclear']
    var_cost_unmet_demand = assumptions['var_cost_unmet_demand']
    var_cost_dispatch_from_storage = assumptions['var_cost_dispatch_from_storage']
    var_cost_dispatch_to_storage = assumptions['var_cost_dispatch_to_storage']
    var_cost_storage = assumptions['var_cost_storage'] # variable cost of using storage capacity
    
    # storage charging efficiency
    storage_charging_efficiency = assumptions['storage_charging_efficiency']
    
    # technologies in system
    system_components = assumptions['system_components']
    
    # initial capacities
    capacity_natgas_in = assumptions['capacity_natgas']     # set to numbers if goal is to set capacity rather than optimize it
    capacity_nuclear_in = assumptions['capacity_nuclear']   # set to numbers if goal is to set capacity rather than optimize it
    capacity_solar_in = assumptions['capacity_solar']       # set to numbers if goal is to set capacity rather than optimize it
    capacity_wind_in = assumptions['capacity_wind']         # set to numbers if goal is to set capacity rather than optimize it
    capacity_storage_in = assumptions['capacity_storage']   # set to numbers if goal is to set capacity rather than optimize it

    num_time_periods = demand_series.size
    start_time = time.time()
        
    # -------------------------------------------------------------------------
    # construct optimization problem
        
    # initialize objective function and constraints
    fcn2min = 0
    constraints = []
    
    # set decision variables and constraints for each technology
    
    # generation technology
        # capacity_[generation] = installed capacity for each generation technology [kW]
        # dispatch_[generation] = energy generated to meet demand at all timesteps (timestep size = 1hr) from each generator [kWh]
        # curtailment_[generation] = energy generated but unused ("curtailed") at all timesteps, specifically from solar, wind, and nuclear
    
    # storage
    # note to self: dispatch from/to storage in kW or kWh?
        # capacity_storage = deployed (installed) size of energy storage [kWh]
        # energy_storage = state of charge at all timesteps [kWh]
        # dispatch_to_storage = charging energy flow for energy storage (grid -> storage) at all timesteps [kW]
        # dispatch_from_storage = discharging energy flow for energy storage (grid <- storage) at all timesteps [kW]
    
    # unmet demand
        # dispatch_unmet_demand = unmet demand at all timesteps as determined from energy balance [kWh]

    #---------------------- natural gas ---------------------------------------

    if 'natural_gas' in system_components:
        # decision variables
            # natural gas is considered fully dispatchable in this model - no curtailment
        capacity_natgas = cvx.Variable(1)
        dispatch_natgas = cvx.Variable(num_time_periods)
        # constraints
        constraints += [
                capacity_natgas >= 0,
                dispatch_natgas >= 0,
                dispatch_natgas <= capacity_natgas
                ]
        if capacity_natgas_in >= 0:
            constraints += [ capacity_natgas == capacity_natgas_in ]
        # objective function
            # contribution to system cost from natural gas
        fcn2min += capacity_natgas * fix_cost_natgas + \
            cvx.sum_entries(dispatch_natgas * var_cost_natgas)/num_time_periods
    else:
        # if natural gas not in system, set capacity and dispatch to zero
        capacity_natgas = 0
        dispatch_natgas = np.zeros(num_time_periods)
        
    #---------------------- solar ---------------------------------------------
        
    if 'solar' in system_components:
        # decision variables
        capacity_solar = cvx.Variable(1)
        dispatch_solar = cvx.Variable(num_time_periods)     # "used" generation
        curtailment_solar = cvx.Variable(num_time_periods)  # "unused" generation
        # constraints
        constraints += [
                capacity_solar >= 0,
                dispatch_solar >= 0, 
                curtailment_solar >= 0,
                # total generation == capacity * capacity factor
                    # solar is non-dispatchable
                    # in current model, solar capacity factors ("solar_series") from Shaner et al., 2018
                dispatch_solar + curtailment_solar == capacity_solar * solar_series
                ]
        if capacity_solar_in >= 0:
            constraints += [ capacity_solar == capacity_solar_in ]
        # objective function
            # contribution to system cost from solar
            # pay for installed capacity and total generation at each timestep (dispatch + curtailment)
        fcn2min += capacity_solar * fix_cost_solar + \
            cvx.sum_entries(dispatch_solar * var_cost_solar)/num_time_periods + \
            cvx.sum_entries(curtailment_solar * var_cost_solar)/num_time_periods
    else:
        # if solar not in system, set capacity, dispatch, and curtailment to zero
        capacity_solar = 0
        dispatch_solar = np.zeros(num_time_periods)
        curtailment_solar = np.zeros(num_time_periods)
        
    #---------------------- wind ----------------------------------------------
        
    if 'wind' in system_components:
        # decision variables
        capacity_wind = cvx.Variable(1)
        dispatch_wind = cvx.Variable(num_time_periods)      # "used" generation
        curtailment_wind = cvx.Variable(num_time_periods)   # "unused" generation
        # constraints
        constraints += [
                capacity_wind >= 0,
                dispatch_wind >= 0, 
                curtailment_wind >= 0,
                # total generation == capacity * capacity factor
                    # wind is non-dispatchable
                    # in current model, wind capacity factors ("solar_series") from Shaner et al., 2018
                dispatch_wind + curtailment_wind == capacity_wind * wind_series 
                ]
        if capacity_wind_in >= 0:
            constraints += [ capacity_wind == capacity_wind_in ]
        # objective function
            # contribution to system cost from wind
            # pay for installed capacity and total generation at each timestep (dispatch + curtailment)
        fcn2min += capacity_wind * fix_cost_wind + \
            cvx.sum_entries(dispatch_wind * var_cost_wind)/num_time_periods + \
            cvx.sum_entries(curtailment_wind * var_cost_wind)/num_time_periods
    else:
        # if wind not in system, set capacity, dispatch, and curtailment to zero
        capacity_wind = 0
        dispatch_wind = np.zeros(num_time_periods)
        curtailment_wind = np.zeros(num_time_periods)
        
    #---------------------- nuclear -------------------------------------------
        
    if 'nuclear' in system_components:
        # decision variables
        capacity_nuclear = cvx.Variable(1)
        dispatch_nuclear = cvx.Variable(num_time_periods)       # "used" generation
        curtailment_nuclear = cvx.Variable(num_time_periods)    # "unused" generation
        # constraints
        if flex_nuc_flag:   # if nuclear is assumed to be fully flexible / rampable (flex_nuc_flag = 1)
            constraints += [
                    capacity_nuclear >= 0,
                    dispatch_nuclear >= 0, 
                    curtailment_nuclear >= 0,
                    # total generation <= capacity
                    dispatch_nuclear + curtailment_nuclear <= capacity_nuclear
                    ]
        else:               # if nuclear is assumed to generate at full capacity (flex_nuc_flag = 0)
            constraints += [
                    capacity_nuclear >= 0,
                    dispatch_nuclear >= 0, 
                    curtailment_nuclear >= 0,
                    # total generation == capacity
                        # underlying assumption: capacity factor = 1 at all times
                    dispatch_nuclear + curtailment_nuclear == capacity_nuclear
                    ]

        if capacity_nuclear_in >= 0:
            constraints += [ capacity_nuclear == capacity_nuclear_in ]
        # objective function
            # contribution to system cost from wind
            # pay for installed capacity and total generation at each timestep (dispatch + curtailment)
        fcn2min += capacity_nuclear * fix_cost_nuclear + \
            cvx.sum_entries(dispatch_nuclear * var_cost_nuclear)/num_time_periods + \
            cvx.sum_entries(curtailment_nuclear * var_cost_nuclear)/num_time_periods
    else:
        # if nuclear not in system, set capacity, dispatch, and curtailment to zero
        capacity_nuclear = 0
        dispatch_nuclear = np.zeros(num_time_periods)
        curtailment_nuclear = np.zeros(num_time_periods)
        
    #---------------------- storage -------------------------------------------
        
    if 'storage' in system_components:
        
        # decision variables
        capacity_storage = cvx.Variable(1)
        dispatch_to_storage = cvx.Variable(num_time_periods)
        dispatch_from_storage = cvx.Variable(num_time_periods)
        energy_storage = cvx.Variable(num_time_periods)
        
        # constraints
        constraints += [
                capacity_storage >= 0,
                dispatch_to_storage >= 0, 
                dispatch_from_storage >= 0,
                energy_storage >= 0,
                energy_storage <= capacity_storage
                ]
        if capacity_storage_in >= 0:
            constraints += [ capacity_storage == capacity_storage_in ]
        # charge balance between consecutive timesteps and between simulation cycles (between last timestep in current cycle and first timestep in next cycle)
            # essentially: total disptach from storage = total dispatch to storage within one simulation cycle
        for i in xrange(num_time_periods):
            constraints += [
                    energy_storage[(i+1) % num_time_periods] == energy_storage[i] + storage_charging_efficiency * dispatch_to_storage[i] - dispatch_from_storage[i]
                    ]
        # set initial energy stored to zero
#        constraints += [energy_storage[0] == 0.0]
                    
        # objective function
            # contribution to system cost from storage
            # pay for installed capacity, energy stored at each timestep, energy flows (dispatch to and from storage) at each tiemstep  
        fcn2min += capacity_storage * fix_cost_storage +  \
            cvx.sum_entries(energy_storage * var_cost_storage)/num_time_periods  + \
            cvx.sum_entries(dispatch_to_storage * var_cost_dispatch_to_storage)/num_time_periods + \
            cvx.sum_entries(dispatch_from_storage * var_cost_dispatch_from_storage)/num_time_periods 

    else:
        # if storage not in system, set capacity and dispatch to zero
        capacity_storage = 0
        dispatch_to_storage = np.zeros(num_time_periods)
        dispatch_from_storage = np.zeros(num_time_periods)
        energy_storage = np.zeros(num_time_periods)
       
    #---------------------- unmet demand --------------------------------------
       
    if 'unmet_demand' in system_components:
        # decision variables
        dispatch_unmet_demand = cvx.Variable(num_time_periods)
        # constraints
        constraints += [
                dispatch_unmet_demand >= 0
                ]
        # objective function
        fcn2min += cvx.sum_entries(dispatch_unmet_demand * var_cost_unmet_demand)/num_time_periods
    else:
        # if unmet demand not in system, set dispatch to zero
        dispatch_unmet_demand = np.zeros(num_time_periods)
        
    #---------------------- system energy balance -----------------------------
    
    # dispatch (amount generated for meeting demand) = demand at all timesteps
    constraints += [
            dispatch_natgas + dispatch_solar + dispatch_wind + dispatch_nuclear + dispatch_from_storage +  dispatch_unmet_demand  == 
                demand_series + dispatch_to_storage
            ]    
    
    # -------------------------------------------------------------------------
    # solve optimization problem
    
    # objective function = minimize hourly system cost
    obj = cvx.Minimize(fcn2min)

    # optimization problem to be solved
    prob = cvx.Problem(obj, constraints)
    
    # solver parameters
#    prob.solve(solver = 'GUROBI')   # use default settings
#    prob.solve(solver = 'GUROBI',BarConvTol = 1e-8, FeasibilityTol = 1e-6)     # Gurobi default settings
    prob.solve(solver = 'GUROBI',BarConvTol = 1e-11, FeasibilityTol = 1e-6)
#    prob.solve(solver = 'GUROBI',BarConvTol = 1e-11, FeasibilityTol = 1e-9)
#    prob.solve(solver = 'GUROBI',BarConvTol = 1e-10, FeasibilityTol = 1e-8)

    # -------------------------------------------------------------------------

    end_time = time.time()
    if verbose:
        print 'system cost ', prob.value, '$/kWh'
        print 'runtime ', (end_time - start_time), 'seconds'
        
    # -------------------------------------------------------------------------
#    # calculate curtailment        
#        
#    dispatch_curtailment = np.zeros(num_time_periods)
#    if 'wind' in system_components :
#        dispatch_curtailment = dispatch_curtailment + capacity_wind.value.flatten() * wind_series - dispatch_wind.value.flatten()
#    if 'solar' in system_components:
#        dispatch_curtailment = dispatch_curtailment + capacity_solar.value.flatten() * solar_series - dispatch_solar.value.flatten()
#    if 'nuclear' in system_components:
#        dispatch_curtailment = dispatch_curtailment + capacity_nuclear.value.flatten() - dispatch_nuclear.value.flatten()      
#        
#    dispatch_curtailment = np.array(dispatch_curtailment.flatten())
    
    # -----------------------------------------------------------------------------
    # summary of results (to be written to .csv files)
    
    result = {
            'system_cost':prob.value,
            'problem_status':prob.status,
#            'dispatch_curtailment':dispatch_curtailment
            }
    
    if 'natural_gas' in system_components:
        result['capacity_natgas'] = np.asscalar(capacity_natgas.value)
        result['dispatch_natgas'] = np.array(dispatch_natgas.value).flatten()
    else:
        result['capacity_natgas'] = capacity_natgas
        result['dispatch_natgas'] = dispatch_natgas

    if 'solar' in system_components:
        result['capacity_solar'] = np.asscalar(capacity_solar.value)
        result['dispatch_solar'] = np.array(dispatch_solar.value).flatten()
        result['curtailment_solar'] = np.array(curtailment_solar.value).flatten()
    else:
        result['capacity_solar'] = capacity_solar
        result['dispatch_solar'] = dispatch_solar
        result['curtailment_solar'] = curtailment_solar

    if 'wind' in system_components:
        result['capacity_wind'] = np.asscalar(capacity_wind.value)
        result['dispatch_wind'] = np.array(dispatch_wind.value).flatten()
        result['curtailment_wind'] = np.array(curtailment_wind.value).flatten()
    else:
        result['capacity_wind'] = capacity_wind
        result['dispatch_wind'] = dispatch_wind
        result['curtailment_wind'] = curtailment_wind

    if 'nuclear' in system_components:
        result['capacity_nuclear'] = np.asscalar(capacity_nuclear.value)
        result['dispatch_nuclear'] = np.array(dispatch_nuclear.value).flatten()
        result['curtailment_nuclear'] = np.array(curtailment_nuclear.value).flatten()
    else:
        result['capacity_nuclear'] = capacity_nuclear
        result['dispatch_nuclear'] = dispatch_nuclear
        result['curtailment_nuclear'] = curtailment_nuclear

    if 'storage' in system_components:
        result['capacity_storage'] = np.asscalar(capacity_storage.value)
        result['dispatch_to_storage'] = np.array(dispatch_to_storage.value).flatten()
        result['dispatch_from_storage'] = np.array(dispatch_from_storage.value).flatten()
        result['energy_storage'] = np.array(energy_storage.value).flatten()
    else:
        result['capacity_storage'] = capacity_storage
        result['dispatch_to_storage'] = dispatch_to_storage
        result['dispatch_from_storage'] = dispatch_from_storage
        result['energy_storage'] = energy_storage
        
    if 'unmet_demand' in system_components:
        result['dispatch_unmet_demand'] = np.array(dispatch_unmet_demand.value).flatten()
    else:
        result['dispatch_unmet_demand'] = dispatch_unmet_demand

    return result
  