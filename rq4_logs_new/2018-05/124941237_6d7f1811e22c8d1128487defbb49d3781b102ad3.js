// This turns a microbit into a beacon for the
// beacon-letters game It is based on the example at
// https://makecode.microbit.org/projects/hot-or-cold
radio.setGroup(1)
radio.setTransmitSerialNumber(true)
radio.setTransmitPower(1)
radio.setGroup(1)
basic.forever(() => {
    radio.sendString("a")
    basic.showIcon(IconNames.Heart)
    basic.showIcon(IconNames.Surprised)
})