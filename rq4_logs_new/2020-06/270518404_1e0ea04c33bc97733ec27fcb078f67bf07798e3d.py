import Adafruit_BBIO.ADC as ADC
from time import sleep
ADC.setup()


ui1 = "P9_39"
ui2 = "P9_40"
ui3 = "P9_37"
ui4 = "P9_38"
ui5 = "P9_33"
ui6 = "P9_36"
ui7 = "P9_35"

# UI Reading constants from the calibration procedure
uiReading = [0.1, 0.2, 0.398, 0.697, 0.995, 1.98, 2.98, 3.98, 5.97, 7.96, 9.95]
ui1Raw = [0.028, 0.0364, 0.053, 0.0789, 0.105, 0.194, 0.2864, 0.3797, 0.5673, 0.7556, 0.9431]
ui2Raw = [0.028, 0.0364, 0.053, 0.0789, 0.105, 0.194, 0.2864, 0.3797, 0.5673, 0.7556, 0.9431]
ui3Raw = [0.028, 0.0364, 0.053, 0.0789, 0.105, 0.194, 0.2864, 0.3797, 0.5673, 0.7556, 0.9431]
ui4Raw = [0.028, 0.0364, 0.053, 0.0789, 0.105, 0.194, 0.2864, 0.3797, 0.5673, 0.7556, 0.9431]
ui5Raw = [0.028, 0.0364, 0.053, 0.0789, 0.105, 0.194, 0.2864, 0.3797, 0.5673, 0.7556, 0.9431]
ui6Raw = [0.028, 0.0364, 0.053, 0.0789, 0.105, 0.194, 0.2864, 0.3797, 0.5673, 0.7556, 0.9431]
ui7Raw = [0.028, 0.0364, 0.053, 0.0789, 0.105, 0.194, 0.2864, 0.3797, 0.5673, 0.7556, 0.9431]

# UI Raw reading dictionary
rawReads = {
    "UI1": ui1Raw,
    "UI2": ui2Raw,
    "UI3": ui3Raw,
    "UI4": ui4Raw,
    "UI5": ui5Raw,
    "UI6": ui6Raw,
    "UI7": ui7Raw,
}


# Scaling function
def uiScale(port, value):
    uiRaw = rawReads.get(port, ui1Raw)  # UI1 default values
    print("--------------------------------------------------------")
    print("Input", value)
    if value <= uiRaw[0]:
        # calculate slope at lower bounds
        slope = (uiReading[0] - uiReading[1]) / (uiRaw[0] - uiRaw[1])
        # interpolate value using the slope
        value = uiReading[0] + ((value - uiRaw[0]) * slope)
        print("Lower bound")
    elif value >= uiRaw[10]:
        # calculate slope at upper bounds
        slope = (uiReading[9] - uiReading[10]) / (uiRaw[9] - uiRaw[10])
        # interpolate value using the slope
        value = uiReading[10] + ((value - uiRaw[10]) * slope)
        print("Upper bound")
    else:
        # interpolate reading when value between uiRaw[i] and uiRaw[i+1]
        print("Mid range")
        for i in range(10):
            # print(i)
            if (value >= uiRaw[i] and value <= uiRaw[i + 1]):
                print("Index", i)
                slope = (uiReading[i] - uiReading[i + 1]) / (uiRaw[i] - uiRaw[i + 1])
                value = uiReading[i] + ((value - uiRaw[i]) * slope)
                print("Slope", slope)
                print("Value", value)
                break

    print("Output", value)
    return value


# testing with defined intputs
# values = [0.01, 0.015, 0.02, 0.04, 0.06, 0.09, 0.15, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 1]
# for x in values:
#        uiScale("UI1", x)

# Scaling using UI actual measurements
while 1:
    ui1Read = uiScale("UI1", ADC.read(ui1))  # Read from UI1 and scale
    print("UI Reading", ui1Read)
    sleep(2)