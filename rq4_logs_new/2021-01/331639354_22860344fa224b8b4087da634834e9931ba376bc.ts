input.calibrateCompass()
let a = input.compassHeading()
if (a == 0) {
    basic.showLeds(`
        . . # . .
        . # # # .
        . . # . .
        . . # . .
        . . # . .
        `)
}
if (a == 90) {
    basic.showLeds(`
        . . . . .
        . . . # .
        # # # # #
        . . . # .
        . . . . .
        `)
}
if (a == 180) {
    basic.showLeds(`
        . . # . .
        . . # . .
        . . # . .
        . # # # .
        . . # . .
        `)
}
if (a == 270) {
    basic.showLeds(`
        . . . . .
        . # . . .
        # # # # #
        . # . . .
        . . . . .
        `)
}
if (a < 90) {
    basic.showLeds(`
        . . . . #
        . . . # .
        . . # . .
        . . . . .
        . . . . .
        `)
}
if (a > 90 && a > 180) {
    basic.showLeds(`
        . . . . .
        . . . . .
        . . # . .
        . . . # .
        . . . . #
        `)
}
if (a > 180 && a > 270) {
    basic.showLeds(`
        . . . . .
        . . . . .
        . . # . .
        . # . . .
        # . . . .
        `)
}
if (a > 270 && a > 360) {
    basic.showLeds(`
        # . . . .
        . # . . .
        . . # . .
        . . . . .
        . . . . .
        `)
}
basic.forever(function () {
	
})