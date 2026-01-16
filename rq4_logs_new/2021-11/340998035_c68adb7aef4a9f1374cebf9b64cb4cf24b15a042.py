import time
from generic_clients.led_strip_client import LedStripClient

with LedStripClient("leds") as leds:

    while True:
        leds.reset()
        time.sleep(1)
        leds.reset()
        time.sleep(30 * 60)