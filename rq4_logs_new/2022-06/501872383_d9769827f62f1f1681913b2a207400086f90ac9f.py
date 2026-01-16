from sx1262 import *

try:
    spi_device = SX1262(24, 13, 18, 22, clk=23, mosi=19, miso=21)
    spi_device.begin(
        freq=923,
        bw=500.0,
        sf=12,
        cr=8,
        syncWord=0x12,
        power=-5,
        currentLimit=60.0,
        preambleLength=8,
        implicit=False,
        implicitLen=0xFF,
        crcOn=True,
        txIq=False,
        rxIq=False,
        tcxoVoltage=1.7,
        useRegulatorLDO=False,
        blocking=True,
    )

    spi_device.send(b"Hello World!")

finally:
    Pin.cleanup()