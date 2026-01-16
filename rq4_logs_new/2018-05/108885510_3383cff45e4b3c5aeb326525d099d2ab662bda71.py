# -*- coding: utf-8 -*-

import logging
import struct
import time
import sys

import serial

"""Enumeration of SDS011 commands"""
command = {
    "ReportMode": 2,
    "Request": 4,
    "DeviceId": 5,
    "WorkState": 6,
    "Firmware": 7,
    "DutyCycle": 8
}

"""Command to get the current configuration or set it"""
command_mode = {
    "Getting": 0,
    "Setting": 1
}

"""
Report modes of the sensor:
In passive mode one has to send a request command,
in order to get the measurement values as a response.
"""
report_mode = {
    "Initiative": 0,
    "Passive": 1
}

"""
The work states:
In sleeping mode it does not send any data, the fan is turned off.
To get data one has to wake it up.
"""
work_state = {
    "Sleeping": 0,
    "Measuring": 1
}

"""
The unit of the measured values.
Two modes are implemented:
The default mode is MassConcentrationEuropean returning
values in microgram/cubic meter (mg/m³).
The other mode is ParticleConcentrationImperial returning values in
particles / 0.01 cubic foot (pcs/0.01cft).
The concentration is calculated by assuming
different mean sphere diameters of pm10 or pm2.5 particles.
"""
unit_of_measure = {
    # µg / m³, the mode of the sensors firmware
    "MassConcentrationEuropean": 0,
    # pcs/0.01 cft (particles / 0.01 cubic foot )
    "ParticleConcentrationImperial": 1
}


class SDS011(object):
    """
    Class representing the SD011 dust sensor and its methods.
    The device_path on Win is one of your COM ports,
    on Linux it is one of "/dev/ttyUSB..." or "/dev/ttyS..."
    """

    __SerialStart = 0xAA
    __SerialEnd = 0xAB
    __SendByte = 0xB4
    __ResponseByte = 0xC5
    __ReceiveByte = 0xC0
    __ResponseLength = 10
    __CommandLength = 19
    __CommandTerminator = 0xFF

    def __init__(self, device_path, **args):
        """
        The device_path on Win is one of your COM ports.
        On Linux one of "/dev/ttyUSB..." or "/dev/ttyAMA..."
        """
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)

        self.sensor_name = "SDS011"
        self.logger.info("{}: start of the sensor class constructor. The device path is \"{}\"".format(self.sensor_name, device_path))

        self.__timeout = 2
        if 'timeout' in args.keys():  # Serial line read timeout
            self.__timeout = int(args['timeout'])

        self.__unit_of_measure = unit_of_measure["MassConcentrationEuropean"]
        if 'unit_of_measure' in args.keys():
            if args['unit_of_measure'] in unit_of_measure.values():
                self.__unit_of_measure = args['unit_of_measure']
            else:
                raise ValueError("{}: given unit_of_measure value {} is not valid.".format(self.sensor_name, args['unit_of_measure']))

        self.__device_path = device_path
        self.device = None
        try:
            self.device = serial.Serial(device_path,
                                        baudrate=9600, stopbits=serial.STOPBITS_ONE,
                                        parity=serial.PARITY_NONE,
                                        bytesize=serial.EIGHTBITS,
                                        timeout=self.__timeout)
            if self.device.isOpen() is False:
                self.device.open()
            self.logger.debug("{}: serial connection successfully started.".format(self.sensor_name))
        except IOError as e:
            raise IOError("{}: unable to set serial device {}. Error: {}.".format(self.sensor_name, device_path, e.message))

        # TODO: initiate with the values, the sensor has to be queried for
        self.__firmware = None
        self.__report_mode = None
        self.__work_state = None
        self.__duty_cycle = None
        self.__device_id = None
        self.__read_timeout = 0
        self.__duty_cycle_start = time.time()
        self.__read_timeout_drift_percent = 2

        # Within response the device id will be set
        first_response = self.__response()
        if len(first_response) == 0:
            # Device might be sleeping. So wake it up
            self.logger.warning("{}: While constructing the instance the sensor is not responding. Maybe in sleeping mode, in passive mode or in a duty cycle? Will wake it up.".format(self.sensor_name))
            self.__send(command["WorkState"], self.__construct_data(command_mode["Setting"], work_state["Measuring"]))
            self.__send(command["DutyCycle"], self.__construct_data(command_mode["Setting"], 0))

        # At this point, the device is awake for sure. So store his state
        self.__work_state = work_state["Measuring"]
        self.__get_current_config()
        self.logger.info("{}: sensor has firmware: {}".format(self.sensor_name, self.__firmware))
        self.logger.info("{}: sensor report mode: {}".format(self.sensor_name, self.__report_mode))
        self.logger.info("{}: sensor work state: {}".format(self.sensor_name, self.__work_state))
        self.logger.info("{}: sensor duty cycle: {}, None if Zero".format(self.sensor_name, self.__duty_cycle))
        self.logger.info("{}: sensor device ID: {}".format(self.sensor_name, self.device_id))
        self.logger.debug("{}: the constructor is successfully executed.".format(self.sensor_name))

    # Conversion parameters come from:
    # http://ir.uiowa.edu/cgi/viewcontent.cgi?article=5915&context=etd
    def mass2particles(self, pm, value):
        """Convert pm size from µg/m3 back to concentration pcs/0.01sqf"""
        if self.__unit_of_measure == unit_of_measure["MassConcentrationEuropean"]:
            return value
        elif self.__unit_of_measure == unit_of_measure["ParticleConcentrationImperial"]:

            pi = 3.14159
            density = 1.65 * pow(10, 12)

            if pm == 'pm10':
                radius = 2.60
            elif pm == 'pm2.5':
                radius = 0.44
            else:
                raise RuntimeError('{}: wrong Mass2Particle parameter value for pm type. "{}" given, "pm10" or "pm2.5" expected.'.format(self.sensor_name, pm))
            radius *= pow(10, -6)
            volume = (4.0 / 3.0) * pi * pow(radius, 3)
            mass = density * volume
            k = 3531.5
            concentration = value / (k * mass)
            return int(concentration + 0.5)

    def __del__(self):
        if self.device is not None:
            self.device.close()

    # Device path
    @property
    def device_path(self):
        return self.__device_path

    # Report mode
    @property
    def report_mode(self):
        return self.__report_mode

    @report_mode.setter
    def report_mode(self, value):
        if value in command_mode.values():
            self.__send(command["ReportMode"], self.__construct_data(command_mode["Setting"], value))
            self.__report_mode = value
            self.logger.info("{}: report mode setted to {}".format(self.sensor_name, value))
        else:
            raise ValueError("{}: report mode value not valid.".format(self.sensor_name))

    # Work state
    @property
    def work_state(self):
        return self.__work_state

    @work_state.setter
    def work_state(self, value):
        if value in work_state.values():
            self.__send(command["WorkState"], self.__construct_data(command_mode["Setting"], value))
            self.__work_state = value
            self.logger.info("{}: sensor work state setted to {}.".format(self.sensor_name, value))
        else:
            raise ValueError("{}: work state value not valid.".format(self.sensor_name))

    # Duty cycle
    @property
    def duty_cycle(self):
        return self.__duty_cycle

    @duty_cycle.setter
    def duty_cycle(self, value):
        if isinstance(value, int):
            if value < 0 or value > 30:
                raise ValueError("{}: duty cycle has to be between 0 and 30 inclusive!".format(self.sensor_name))
            self.__send(command["DutyCycle"], self.__construct_data(command_mode["Setting"], value))
            self.__duty_cycle = value

            # Calculate new timeout value
            self.__read_timeout = self.__calculate_read_timeout(value)
            self.__duty_cycle_start = time.time()
            self.logger.info("{}: set duty cycle timeout to {}.".format(self.sensor_name, self.__read_timeout))
            self.logger.info("{}: set duty cycle to {}.".format(self.sensor_name, value))
            self.__get_current_config()
        else:
            raise TypeError("{}: duty cycle should be of type int.".format(self.sensor_name))

    @property
    def device_id(self):
        return "{0:02x}{1:02x}".format(self.__device_id[0], self.__device_id[1]).upper()

    @property
    def firmware(self):
        return self.__firmware

    @property
    def unit_of_measure(self):
        return self.__unit_of_measure

    @property
    def timeout(self):
        return self.__timeout

    def __construct_data(self, cmd_mode, cmd_value):
        """
        Construct a data byte array from cmd_mode and cmd_value.
        cmd_value has to be CommandMode type and cmd_value int.
        Returns byte array of length 2.
        """
        if cmd_mode not in command_mode.values():
            raise TypeError("%s: specified cmd_mode is not valid.", self.sensor_name)
        if not isinstance(cmd_value, int):
            raise TypeError("%s: cmd_value must be of type %s.", self.sensor_name, type(int))

        ret_val = bytearray()
        ret_val.append(cmd_mode)
        ret_val.append(cmd_value)
        return ret_val

    def __get_current_config(self):
        """
        Get the sensor status at construction time of this instance:
        the current status of the sensor.
        """
        self.logger.debug("{}: getting current sensor configuration.".format(self.sensor_name))
        # Getting the duty cycle
        self.logger.debug("{}: getting current sensor duty cycle.".format(self.sensor_name))
        response = self.__send(command["DutyCycle"], self.__construct_data(command_mode["Getting"], 0))
        if response is not None and len(response) > 0:
            self.__duty_cycle = response[1]
            self.__read_timeout = self.__calculate_read_timeout(self.__duty_cycle)
            self.__duty_cycle_start = time.time()
        else:
            raise RuntimeError("{}: duty cycle is not detectable.".format(self.sensor_name))

        # Getting report mode
        self.logger.debug("{}: getting current sensor report mode.".format(self.sensor_name))
        response = None
        response = self.__send(command["ReportMode"], self.__construct_data(command_mode["Getting"], 0))
        if response is not None and len(response) > 0:
            self.__report_mode = response[1]
        else:
            raise RuntimeError("{}: report mode is not detectable.".format(self.sensor_name))

        # Getting firmware
        self.logger.debug("{}: getting current sensor firmware version.".format(self.sensor_name))
        response = None
        response = self.__send(command["Firmware"], self.__construct_data(command_mode["Getting"], 0))
        if response is not None and len(response) > 0:
            self.__firmware = "{0:02d}{1:02d}{2:02d}".format(response[0], response[1], response[2])
        else:
            raise RuntimeError("{}: firmware is not detectable.".format(self.sensor_name))

    def __calculate_read_timeout(self, timeout_value):
        new_timeout = 60 * timeout_value + self.__read_timeout_drift_percent / 100 * 60 * timeout_value
        if new_timeout != 0:
            self.logger.info("{}: new timeout calculated for specified timeout value {}: {}".format(self.sensor_name, timeout_value, new_timeout))
        return new_timeout

    def get_values(self):
        """Get the sensor response and return measured value of PM10 and PM25"""
        if self.__work_state == work_state["Sleeping"]:
            raise RuntimeError("{}: the sensor is sleeping and will not send any values. Will wake it up first.".format(self.sensor_name))
        if self.__report_mode == report_mode["Passive"]:
            raise RuntimeError("{}: the sensor is in passive report mode and will not automatically send values. You need to call request() to get values.".format(self.sensor_name))

        self.__duty_cycle_start = time.time()
        while self.duty_cycle == 0 or time.time() < self.__duty_cycle_start + self.__read_timeout:
            response_data = self.__response()
            if len(response_data) > 0:
                self.logger.info("{}: received response from sensor {} bytes.".format(self.sensor_name, len(response_data)))
            return self.__extract_values_from_response(response_data)
        raise IOError("{}: no data within read timeout of {} has been received.".format(self.sensor_name, self.__read_timeout))

    def request(self):
        """Request measurement data as a tuple from sensor when its in passive query mode"""
        response = self.__send(command["Request"], bytearray())
        ret_val = self.__extract_values_from_response(response)
        return ret_val

    def __extract_values_from_response(self, response_data):
        """Extracts the value of PM25 and PM10 from sensor response"""
        data = response_data[2:6]
        value_of_2point5micro = None
        value_of_10micro = None
        if len(data) == 4:
            value_of_2point5micro = self.mass2particles(
                'pm2.5', float(data[0] + data[1] * 256) / 10.0)
            value_of_10micro = self.mass2particles(
                'pm10', float(data[2] + data[3] * 256) / 10.0)
            self.logger.info("{}: get_values successful executed.".format(self.sensor_name))
            if self.duty_cycle != 0:
                self.__duty_cycle_start = time.time()
            return value_of_10micro, value_of_2point5micro
        elif self.duty_cycle == 0:
            raise ValueError("{}: data is missing.".format(self.sensor_name))

    def __send(self, cmd_val, data):
        """The method for sending commands to the sensor and returning the response"""
        # Proof the input
        if cmd_val not in command.values():
            raise ValueError("{}: the provided command value {} is not valid.".format(self.sensor_name, cmd_val))
        if not isinstance(data, bytearray):
            raise TypeError("{}: command data must be of type byte array.".format(self.sensor_name))

        # Initialise the command bytes array
        bytes_to_send = bytearray()
        bytes_to_send.append(self.__SerialStart)
        bytes_to_send.append(self.__SendByte)
        bytes_to_send.append(cmd_val)

        # Add data and set zero to the remainder
        for i in range(0, 12):
            if i < len(data):
                bytes_to_send.append(data[i])
            else:
                bytes_to_send.append(0)

        # Last two bytes before the checksum is the CommandTerminator
        # TODO : rename command terminator to sensor ID
        bytes_to_send.append(self.__CommandTerminator)
        bytes_to_send.append(self.__CommandTerminator)

        # Calculate and append the checksum
        checksum = self.__checksum_make(bytes_to_send)
        bytes_to_send.append(checksum % 256)

        # Append the terminator for serial message
        bytes_to_send.append(self.__SerialEnd)

        self.logger.info("{}: sending {} {} command with {} message.".format(self.sensor_name, command_mode.keys()[command_mode.values().index(bytes_to_send[3])], command.keys()[command.values().index(cmd_val)], ":".join("%02x" % b for b in bytes_to_send)))

        if len(bytes_to_send) != self.__CommandLength:
            raise IOError("{}: sent {} bytes, expected {}.".format(self.sensor_name, len(bytes_to_send), self.__CommandLength))

        # Send the command
        written_bytes = self.device.write(bytes_to_send)
        self.device.flush()

        if written_bytes != len(bytes_to_send):
            raise IOError("{}: not all bytes written.".format(self.sensor_name))

        # Check the received values
        received = self.__response(cmd_val)

        if len(received) != self.__ResponseLength:
            raise IOError("{}: received {} bytes, expected {}.".format(self.sensor_name, len(received), self.__ResponseLength))

        if len(received) == 0:
            raise IOError("{}: sensor is not responding.".format(self.sensor_name))

        # When no command or command is request command,
        # second byte has to be ReceiveByte
        if (cmd_val is None or cmd_val == command["Request"]) and received[1] != self.__ReceiveByte:
            raise ValueError("{}: expected to receive value {:#X} on a value request. Received: \"{}\".".format(self.sensor_name, self.__ReceiveByte, received[1]))

        # Check, if the response is response of the command, except request command
        if cmd_val != command["Request"]:
            if received[2] != cmd_val:
                raise ValueError("{}: sensor response does not belong to the command sent before.".format(self.sensor_name))
            else:
                return received[3: -2]
        else:
            return received

    def __response(self, cmd_val=None):
        """
        Get and check the response from the sensor.
        Response can be the response of a command sent or
        just the measurement data, while sensor is in report mode Initiative.
        """
        # Receive the response while listening serial input
        bytes_received = bytearray(1)
        while True:
            one_byte = self.device.read(1)
            '''If no bytes are read the sensor might be in sleep mode.
            It makes no sense to raise an exception here. The raise condition
            should be checked in a context outside of this function.'''
            if len(one_byte) > 0:
                bytes_received[0] = ord(one_byte)
                # if this is true, serial data is coming in
                if bytes_received[0] == self.__SerialStart:
                    single_byte = self.device.read(1)
                    if ((cmd_val is not None and cmd_val != command["Request"]) and ord(single_byte) == self.__ResponseByte) or ((cmd_val is None or cmd_val is command["Request"]) and ord(single_byte) == self.__ReceiveByte):
                        bytes_received.append(ord(single_byte))
                        break
            else:
                if self.__duty_cycle == 0:
                    self.logger.error("{}: a sensor response has not arrived within timeout limit. If the sensor is in sleeping mode wake it up first! Returning an empty byte array as response!".format(self.sensor_name))
                else:
                    self.logger.info("{}: no response. Expected while in duty cycle.".format(self.sensor_name))
                return bytearray()

        response_bytes = struct.unpack('BBBBBBBB', self.device.read(8))
        bytes_received.extend(response_bytes)

        if cmd_val is not None and cmd_val != command["Request"]:

            if bytes_received[1] is not self.__ResponseByte:
                raise IOError("{}: no ResponseByte found in the response.".format(self.sensor_name))

            if bytes_received[2] != cmd_val:
                raise IOError("{}: third byte of serial data \"{}\" received is not the expected response to the previous command: \"{}\"".format(self.sensor_name, bytes_received[2], cmd_val.name))

        if cmd_val is None or cmd_val == command["Request"]:
            if bytes_received[1] is not self.__ReceiveByte:
                raise IOError("{}: received byte not found in the response.".format(self.sensor_name))

        # Evaluate checksum
        if self.__checksum_make(bytes_received[0:-2]) != bytes_received[-2]:
            raise IOError("{}: checksum of received data is invalid.".format(self.sensor_name))

        # Set device_id if device id is not initialized or proof it, if it's not None
        if self.__device_id is None:
            self.__device_id = bytes_received[-4:-2]
        elif self.__device_id is not None and not self.__device_id.__eq__(bytes_received[-4:-2]):
            raise ValueError("{}: data received ({}) does not belong to this device with id {}.".format(self.sensor_name, bytes_received, self.__device_id))

        self.logger.info("{}: the response was successful with message {}.".format(self.sensor_name, "".join("%02x:" % b for b in bytes_received)))
        return bytes_received

    def reset(self):
        """
        Sets Report mode to Initiative. Work state to Measuring and Duty cycle to 0
        """
        self.work_state = work_state["Measuring"]
        self.report_mode = report_mode["Initiative"]
        self.duty_cycle = 0
        self.logger.info("{}: sensor resetted.".format(self.sensor_name))

    def __checksum_make(self, data):
        """
        Generates the checksum for data to be sent or received from the sensor.
        The data has to be of type byte array and must start with 0xAA,
        followed by 0xB4 or 0xC5 or 0xC0 as second byte.
        The sequence must end before the position of the checksum.
        """
        self.logger.info("{}: building the checksum for bytes {}.".format(self.sensor_name, ":".join("%02x" % b for b in data)))

        if len(data) not in (self.__CommandLength - 2, self.__ResponseLength - 2):
            raise ValueError("{}: length data has to be {} or {}.".format(self.sensor_name, self.__CommandLength - 2, self.__ResponseLength))

        if data[0] != self.__SerialStart:
            raise ValueError("{}: data is missing the start byte.".format(self.sensor_name))

        if data[1] not in (self.__SendByte, self.__ResponseByte, self.__ReceiveByte):
            raise ValueError("{}: data is missing SendByte, ReceiveByte or ReceiveValue-Byte".format(self.sensor_name))

        if data[1] != self.__ReceiveByte and data[2] not in command.values():
            raise ValueError("{}: the data command byte value \"{}\" is not valid.".format(self.sensor_name, data[2]))

        # Build checksum for data to send or receive
        checksum = 0
        for i in range(2, len(data)):
            checksum = checksum + data[i]
        checksum = checksum % 256

        self.logger.info("{}: checksum calculated {} for bytes {}.".format(self.sensor_name, "%02x" % checksum, ":".join("%02x" % b for b in data)))
        return checksum