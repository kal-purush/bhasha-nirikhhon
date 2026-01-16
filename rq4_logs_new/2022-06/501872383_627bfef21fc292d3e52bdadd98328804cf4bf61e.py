import logging
from typing import Callable, Union
import Jetson.GPIO as GPIO


class Pin:
    __INITIALIZED__: bool = False
    IN: int = GPIO.IN
    OUT: int = GPIO.OUT
    HIGH: int = GPIO.HIGH
    LOW: int = GPIO.LOW
    IRQ_RISING = GPIO.RISING
    IRQ_FALLING = GPIO.FALLING
    IRQ_BOTH = GPIO.BOTH

    def __init__(
        self,
        pinNumber: Union[str, int],
        direction: int,
        initial_state=None,
    ) -> None:
        self.pinNumber = pinNumber
        self.direction = direction
        self.state = initial_state

        if Pin.__INITIALIZED__ == False:
            Pin.initialize()

        GPIO.setup(self.pinNumber, self.direction, initial=self.state)

    def value(self, new_state=None):
        """Set state of pin

        Args:
            new_state (Union[int, bool]): desired state of pin

        Returns:
            int: Current state of pin. 1=HIGH, 0=LOW
        """
        if new_state is not None:
            GPIO.output(self.pinNumber, new_state)
        return GPIO.input(self.pinNumber)

    def set_irq(
        self, trigger: int = IRQ_RISING, handler: Callable[..., None] = lambda: None
    ):
        """Initialize interrupt callback

        Args:
            trigger (int, optional): Signal to trigger interrupt on. Defaults to IRQ_RISING.
            handler (function, optional): function called on interrupt trigger. Defaults to lambda:None.
        """
        # TODO: test this
        logging.debug("UNTESTED IRQ FUNCTION CALLED.")
        GPIO.add_event_detect(self.pinNumber, trigger, callback=handler)

    def clear_irq(self):
        """Disable configured interrupt"""
        GPIO.remove_event_detect(self.pinNumber)

    @staticmethod
    def initialize(mode: int = GPIO.BOARD):
        """Manually initialize GPIO library

        Args:
            mode (int, optional): GPIO pin mode. Defaults to GPIO.BOARD.
        """
        GPIO.setmode(mode)
        Pin.__INITIALIZED__ = True

    @staticmethod
    def cleanup():
        """De-initialize GPIO library. This function must be called on exit of the program."""
        GPIO.cleanup()
        Pin.__INITIALIZED__ = False