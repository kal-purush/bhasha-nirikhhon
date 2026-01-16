# Imports

# Known SDI-12 device addresses
SDI_12_DEVICES = {
    'a' : 'TEROS-12',
    'b' : 'MPS-6',
    'z' : '100K-THERMISTOR'
}

HEADER = "DateTime,Hostname,SDISensor1,VWC(m^3),Temp(°C),EC(dS/m),SDISensor2,WaterPotential(kPa),Temp(°C),AnalogSensor,Voltage(V)\n"

def format_output(data_str):
    # Convert data string into list and trim off the last 7 irrelevant data points
    csv_data = data_str.split(",")[:-7]
    
    # Scan through the data to see if there's a match with known SDI-12 devices
    for address in list(SDI_12_DEVICES.keys()):
        for index in range(len(csv_data)):
            if address == csv_data[index]:
                # If a known SDI-12 address matches with an address in the data, 
                # overwrite the address with a common sensor name
                csv_data[index] = SDI_12_DEVICES[csv_data[index]]

    # Return newly formatted data as a string
    return ",".join(csv_data)

def add_header(data_file):
    # Check to see if the data file is empty

        # If it's empty, write the header
    pass

def voltage_to_kelvin(volts):
    pass


### Test Code ###
def main():
    sensor_reading = "2023-03-21 15:24:30,ccriiob11,a,1932.52,19.2,169.0,b,-10.1,19.1,z,2.734,2.31,2.017,1.89,1.0,0.0,0.0,0.0"
    print(f"Initial sensor reading: {sensor_reading}")

    formatted_data = format_output(sensor_reading)
    print(f"Formatted data: {formatted_data}")
    

if __name__ == "__main__":
    main()