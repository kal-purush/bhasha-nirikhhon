function speed () {
    if (dx == 0) {
        if (dy == 0) {
            LM = -100
            RM = 255
        } else if (dy == 1) {
            LM = 100
            RM = 255
        } else if (dy == 2) {
            LM = -255
            RM = 255
        } else if (dy == 3) {
            LM = 255
            RM = 100
        } else {
            LM = 255
            RM = -100
        }
    } else if (dx == 1) {
        if (dy == 0) {
            LM = -10
            RM = 100
        } else if (dy == 1) {
            LM = 10
            RM = 100
        } else if (dy == 2) {
            LM = -100
            RM = 100
        } else if (dy == 3) {
            LM = 100
            RM = 10
        } else {
            LM = 100
            RM = -10
        }
    } else if (dx == 2) {
        if (dy == 0) {
            LM = 255
            RM = 255
        } else if (dy == 1) {
            LM = 100
            RM = 100
        } else if (dy == 2) {
            LM = 0
            RM = 0
        } else if (dy == 3) {
            LM = -100
            RM = -100
        } else {
            LM = -255
            RM = -255
        }
    } else if (dx == 3) {
        if (dy == 0) {
            LM = 10
            RM = -100
        } else if (dy == 1) {
            LM = -10
            RM = -100
        } else if (dy == 2) {
            LM = 100
            RM = -100
        } else if (dy == 3) {
            LM = -100
            RM = -10
        } else {
            LM = -100
            RM = 10
        }
    } else {
        if (dy == 0) {
            LM = 100
            RM = -255
        } else if (dy == 1) {
            LM = -100
            RM = -255
        } else if (dy == 2) {
            LM = 255
            RM = -255
        } else if (dy == 3) {
            LM = -255
            RM = -100
        } else {
            LM = -255
            RM = 100
        }
    }
}
radio.onReceivedValue(function (name, value) {
    if (name == "dx") {
        dx = value
    } else {
        dy = value
        speed()
    }
})
let dy = 0
let dx = 0
let RM = 0
let LM = 0
radio.setGroup(1)
LM = 0
RM = 0
dx = 0
dy = 0
basic.forever(function () {
    maqueen.motorRun(maqueen.Motors.M1, maqueen.Dir.CW, LM)
    maqueen.motorRun(maqueen.Motors.M2, maqueen.Dir.CW, RM)
})