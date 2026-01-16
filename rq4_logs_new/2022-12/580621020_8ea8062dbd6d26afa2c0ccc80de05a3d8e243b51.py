def moment_loadsol(MA_num, force_vals, mass):
    # This function calculates the achilles tendon load based on loadsol
    # readings from the three sensor loadout. The MA_num variable refers to 
    # which row in the geom_dict variable is used for the calculation. Force_vals
    # are a matrix of loadsol readings from each region.


    geom_dict = [[0.03875, 0.11415, 0.1908, 0.051, 1], [0.0415, 0.12165, 0.2027, 0.054, 1], [0.0425, 0.12665, 0.2137, 0.0575, 1], [0.045, 0.1325, 0.223, 0.06, 1], [0.0475, 0.1400, 0.233, 0.0625, 1]]

    vals = geom_dict[MA_num][:-1]
    COPs = vals - vals[-1]
    COP_real = [vals[0]/2 - vals[-1], vals[0] + vals[1]/2 - vals[-1], vals[0] + vals[1] + vals[2]/2 - vals[-1]]

    moment = force_vals[:, 0] * COP_real[0] + force_vals[:, 1] * COP_real[1] + force_vals[:, 2] * COP_real[2]

    T_F = moment / geom_dict[MA_num][-2]
    T_F_norm = T_F / (9.8 * mass)

    return moment, T_F, T_F_norm