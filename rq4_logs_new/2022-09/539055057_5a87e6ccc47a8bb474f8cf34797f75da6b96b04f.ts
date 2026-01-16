input.onButtonPressed(Button.A, function () {
    Player.change(LedSpriteProperty.Y, 1)
})
input.onGesture(Gesture.TiltLeft, function () {
    Player.change(LedSpriteProperty.X, -1)
})
input.onButtonPressed(Button.AB, function () {
    Number2 = 6
    for (let index = 0; index < 5; index++) {
        Number2 += -1
        basic.showNumber(Number2)
    }
    Enemy = game.createSprite(randint(0, 4), randint(0, 4))
    Player = game.createSprite(2, 2)
    chung += -1
})
input.onButtonPressed(Button.B, function () {
    Player.change(LedSpriteProperty.Y, -1)
})
input.onGesture(Gesture.TiltRight, function () {
    Player.change(LedSpriteProperty.X, 1)
})
let chung = 0
let Player: game.LedSprite = null
let Enemy: game.LedSprite = null
let Number2 = 0
Number2 = 6
for (let index = 0; index < 5; index++) {
    Number2 += -1
    basic.showNumber(Number2)
}
Enemy = game.createSprite(randint(0, 4), randint(0, 4))
Player = game.createSprite(2, 2)
basic.forever(function () {
    if (Enemy.isTouching(Player) && chung == 0) {
        Enemy.delete()
        Player.delete()
        basic.showLeds(`
            . . . . .
            . . . . .
            . . # . .
            . . . . .
            . . . . .
            `)
        basic.pause(100)
        basic.showLeds(`
            . . . . .
            . . # . .
            . # # # .
            . . # . .
            . . . . .
            `)
        basic.pause(100)
        basic.showLeds(`
            . . . . .
            . # . # .
            . . # . .
            . # . # .
            . . . . .
            `)
        basic.pause(100)
        basic.showLeds(`
            . . # . .
            . # . # .
            # . . . #
            . # . # .
            . . # . .
            `)
        basic.showLeds(`
            # . # . #
            . . . . .
            # . . . #
            . . . . .
            # . # . #
            `)
        basic.pause(100)
        basic.showLeds(`
            . . . . .
            . . . . .
            . . . . .
            . . . . .
            . . . . .
            `)
        basic.pause(100)
        music.playMelody("E - E - - D - D ", 400)
        music.playMelody("- C C C - - G C ", 400)
        chung += 1
        basic.pause(100)
    }
    if (chung == 1) {
        basic.showLeds(`
            . . # . .
            . . # # .
            # # # # #
            . . # # .
            . . # . .
            `)
    }
})