"""
This script was written to collect simultaneous data from the max and thermistor sensors.
It is to be used in combination with max_thermistor.ino which is an Arduino script.
The script makes use of the pyserial module.
"""

import argparse
import serial
import time
import h5py

def format_data_file(input_file, output_file, sampling_rate=200):
    try:
        data = np.genfromtxt(input_file, delimiter=',')
    except:
        print("Reading of raw max file {} failed".format(input_file))
        raise

    breath_signal = (data[:,3].flatten()+data[:,2].flatten())/2

    ser = serial.Serial(serial_port, baud_rate)
    data_list = []
    while(True):
       try:
           val = ser.readline().strip().decode("utf-8")

           # Ignore initial message
           if "nitial" in val:
               continue

           # Add timestamp to output
           csv_row = "{}, {}\n".format(time.time(), val)

           data_list.append(csv_row)
           print(csv_row)
       except KeyboardInterrupt:
           break
       except:
           # Ignore bad lines
           pass
    return data_list

def format_data_files(input_files, output_files, sampling_rate=200):

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Format collected max data to be of standard form.')
    parser.add_argument('input', type=str, help='Name of the max_thermistor raw signal file or directory of files to be formatted.')
    parser.add_argument('output', type=str, help='Name of the file or directory to save the data to.')
    parser.add_argument('--sampling_rate', type=int, default=200, help='Sampling rate at which the data was collected (Note that this should be consistent within one dataset).')
    args = parser.parse_args()

    save_path = args.save_path
    serial_port_path = args.serial_port_path
    sampling_rate = args.sampling_rate
    baud_rate = args.baud_rate

    data_list = collect_data(serial_port=serial_port_path, baud_rate=baud_rate, sampling_rate=sampling_rate)
    save_data(save_path, data_list)