function green () {
    range = strip.range(0, 1)
    range.showColor(neopixel.colors(NeoPixelColors.Black))
    range = strip.range(1, 1)
    range.showColor(neopixel.colors(NeoPixelColors.Black))
    range = strip.range(2, 1)
    range.showColor(neopixel.colors(NeoPixelColors.Green))
}
input.onButtonPressed(Button.A, function () {
    pedestrian = 1
})
function traffic_light () {
    basic.showLeds(`
        . # # # .
        # # # # #
        # # # # #
        # # # # #
        . # # # .
        `)
    red()
    basic.pause(10000)
    sonar()
    if (pedestrian == 1) {
        if (sound == 1) {
            green()
            basic.showLeds(`
                # . . . #
                # . . . #
                # . # . #
                # . # . #
                . # . # .
                `)
            for (let index = 0; index < 5; index++) {
                music.playTone(740, music.beat(BeatFraction.Whole))
                basic.pause(450)
                music.playTone(554, music.beat(BeatFraction.Whole))
                basic.pause(450)
            }
            for (let countdown = 0; countdown <= 9; countdown++) {
                basic.showNumber(9 - countdown)
                music.playTone(554, music.beat(BeatFraction.Whole))
            }
            music.playTone(415, music.beat(BeatFraction.Breve))
            yellow()
            basic.showLeds(`
                . # # # .
                # # # # #
                # # # # #
                # # # # #
                . # # # .
                `)
            basic.pause(5000)
            pedestrian = 0
            sound = 0
        } else {
            green()
            basic.showLeds(`
                # . . . #
                # . . . #
                # . # . #
                # . # . #
                . # . # .
                `)
            basic.pause(10000)
            for (let countdown = 0; countdown <= 9; countdown++) {
                basic.showNumber(9 - countdown)
                basic.pause(400)
            }
            yellow()
            basic.showLeds(`
                . # # # .
                # # # # #
                # # # # #
                # # # # #
                . # # # .
                `)
            basic.pause(5000)
            pedestrian = 0
        }
    }
    if (car < 5) {
        green()
        basic.pause(10000)
        yellow()
        basic.pause(5000)
    }
}
input.onButtonPressed(Button.B, function () {
    pedestrian = 1
    sound = 1
})
function yellow () {
    range = strip.range(0, 1)
    range.showColor(neopixel.colors(NeoPixelColors.Black))
    range = strip.range(1, 1)
    range.showColor(neopixel.colors(NeoPixelColors.Yellow))
    range = strip.range(2, 1)
    range.showColor(neopixel.colors(NeoPixelColors.Black))
}
function red () {
    range = strip.range(0, 1)
    range.showColor(neopixel.colors(NeoPixelColors.Red))
    range = strip.range(1, 1)
    range.showColor(neopixel.colors(NeoPixelColors.Black))
    range = strip.range(2, 1)
    range.showColor(neopixel.colors(NeoPixelColors.Black))
}
function sonar () {
    pins.digitalWritePin(DigitalPin.P1, 0)
    control.waitMicros(2)
    pins.digitalWritePin(DigitalPin.P1, 1)
    control.waitMicros(10)
    pins.digitalWritePin(DigitalPin.P1, 0)
    car = pins.pulseIn(DigitalPin.P2, PulseValue.High) / 58
}
let range: neopixel.Strip = null
let strip: neopixel.Strip = null
let car = 0
let sound = 0
let pedestrian = 0
basic.showLeds(`
    . # # # .
    # # # # #
    # # # # #
    # # # # #
    . # # # .
    `)
let power = 1
pedestrian = 0
sound = 0
car = 0
strip = neopixel.create(DigitalPin.P16, 3, NeoPixelMode.RGB)
strip.setBrightness(255)
while (power == 1) {
    traffic_light()
}