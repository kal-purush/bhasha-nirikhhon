function bereken_gemiddelde () {
    gemiddelde = 0
    for (let index = 0; index < 10000; index++) {
        gemiddelde += pins.analogReadPin(AnalogPin.P0)
    }
    gemiddelde = gemiddelde / 10000
    serial.writeValue("g", gemiddelde)
}
input.onButtonPressed(Button.A, function () {
    basic.pause(5000)
    bereken_gemiddelde()
    serial.writeValue("t", gemiddelde + 20)
})
let gemiddelde = 0
basic.showIcon(IconNames.Yes)
basic.forever(function () {
    serial.writeNumbers([pins.analogReadPin(AnalogPin.P0), 405])
    if (pins.analogReadPin(AnalogPin.P0) > 405) {
        basic.showIcon(IconNames.No)
        basic.pause(1000)
        basic.clearScreen()
    }
})