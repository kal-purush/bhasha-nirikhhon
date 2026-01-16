##
# @file
# @author Denise Ratasich
# @date 23.03.2015
#
# @brief Interfacing and monitoring of GPIO pins.
#
# A general purpose input is periodically checked. When the input
# changes (edge can be specified) a function is called (can be set
# during initialization).
##

import RPi.GPIO as gpio
import logging

RISING = 1
FALLING = 0

# @brief Invokes callback function if GPIO triggers.
class EdgeMonitor:

    ##
    # @brief Constructor.
    ##
    def __init__(self, gpio_pin, edge, callback):
	# init attributes
        self._gpio_pin = gpio_pin
        self.setcallback(callback)
        if edge == RISING:
            self._edge = gpio.RISING
        else:
            self._edge = gpio.FALLING

	# set pin connected to sensor as input
	gpio.setmode(gpio.BOARD)
	gpio.setup(self._gpio_pin, gpio.IN, pull_up_down=gpio.PUD_UP)

	logging.debug('EdgeMonitor: ' + str(self.__dict__))

    ##
    # @brief Pass function that should be called when sensor triggers.
    ##
    def setcallback(self, fct):
        if callable(fct):
            self._callback = fct
            logging.debug('EdgeMonitor: ' + str(fct) + ' will be called when edge detected.')
        else:
            raise Exception('EdgeMonitor: The passed parameter is not callable!')

    def gpio_callback(self, gpio_pin):
	logging.debug('EdgeMonitor: edge detected.')
        self._callback()

    ##
    # @brief Periodically check the sensor.
    ##
    def start(self):
	gpio.add_event_detect(self._gpio_pin, self._edge, callback=self.gpio_callback)
        logging.info('EdgeMonitor: started.')