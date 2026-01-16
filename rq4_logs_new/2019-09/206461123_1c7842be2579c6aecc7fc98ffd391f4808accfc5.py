# GV Vbias
# Reading AC current and AC voltage with two lockins

import numpy as np
import time
from qcodes.dataset.measurements import Measurement
from qcodes.dataset.plotting import plot_by_id

def GV_up(station, voltages, amplitude, stanford_gain_V):

    station.lockin_1.amplitude(amplitude)

    time_constant = station.lockin_1.time_constant()

    #print(f'Stanford Gain V ={stanford_gain_V}') TO DO
    #print(f'Stanford Gain I ={stanford_gain_I}')
    print(f'Voltage Max V_max = {voltages[-1]}')

    int_time = 1 #Integration time of the dmm's

    station.dmm1.volt()
    station.dmm1.NPLC(int_time)

    #station.dmm2.volt() 
    #station.dmm2.NPLC(int_time)

    print(f'Integration time = {int_time*0.02} s')

    station.yoko.output('off')
    station.yoko.source_mode("VOLT")
    station.yoko.output('on')

    station.yoko.voltage.step = 5e-3
    station.yoko.voltage.inter_delay = 10e-3

    meas = Measurement()

    meas.register_parameter(station.dmm1.volt)
    meas.register_parameter(station.yoko.voltage)
    meas.register_parameter(station.lockin_1.amplitude)
    meas.register_parameter(station.lockin_1.Y)
    meas.register_parameter(station.lockin_2.Y)
    meas.register_parameter(station.lockin_1.X)
    meas.register_parameter(station.lockin_2.X)
    meas.register_custom_parameter("G", unit="S",setpoints=(station.dmm1.volt,))
    

    with meas.run() as datasaver:

        for v in voltages:

            station.yoko.voltage(v)

            v_dc = station.dmm1.volt()/stanford_gain_V

            time.sleep(5*time_constant)

            current_X_AC = station.lockin_1.X()
            voltage_X_AC = station.lockin_2.X()

            current_Y_AC = station.lockin_1.Y()
            voltage_Y_AC = station.lockin_2.Y()

            G_ac = current_X_AC/voltage_X_AC

            datasaver.add_result(("G",G_ac),
                                (station.dmm1.volt, v_dc),
                                (station.yoko.voltage, v),
                                (station.lockin_1.Y,current_Y_AC),
                                (station.lockin_2.Y,voltage_Y_AC),
                                (station.lockin_1.X,current_X_AC),
                                (station.lockin_2.X,voltage_X_AC))
        ID_exp = datasaver.run_id

    station.yoko.voltage(0)
    plot_by_id(ID_exp)