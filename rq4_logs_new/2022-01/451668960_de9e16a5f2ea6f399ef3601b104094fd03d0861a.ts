let x = 0
let strip = neopixel.create(DigitalPin.P1, 15, NeoPixelMode.RGB)
strip.setPixelColor(0, neopixel.colors(NeoPixelColors.Blue))
strip.show()
basic.forever(function () {
    basic.showNumber(pins.analogReadPin(AnalogPin.P0))
})
basic.forever(function () {
    x = pins.map(
    pins.analogReadPin(AnalogPin.P0),
    0,
    1023,
    0,
    100000
    )
    basic.pause(x)
    strip.rotate(1)
    strip.show()
})