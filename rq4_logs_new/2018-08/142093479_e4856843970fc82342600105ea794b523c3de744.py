# Import the ADS1x15 module.
import Adafruit_ADS1x15


class MultiTurnPot:
    # Reads the multi turn pot and sends an event after a turn has been made.

    def __init__(self, AtoDPin, thresh, potTurned):
        self.AtoDPin = AtoDPin
        self.potTurnedCallback = potTurned
        self.thresh = thresh

        self.gain = 1
        self.thresh = .35
        self.divisor = 16.48
        self.val0

        # Create an ADS1015 ADC (12-bit) instance.
        self.adc = Adafruit_ADS1x15.ADS1015()

    def tick(self):
        # Should be called a 4 Hz
        # Read ADC channel AtoAPin for volume.
        val1 = self.adc.read_adc(self.AtoDPin, gain=self.gain) / self.divisor
        deltaVal = val1 - self.val0
        if deltaVal > self.thresh or deltaVal < -self.thresh:
            # print deltaV
            self.val0 = val1
            self.potTurnedCallback(deltaVal)