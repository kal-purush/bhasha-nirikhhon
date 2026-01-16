def on_button_pressed_a():
    # 値が大きいほど、速く動きます
    pins.analog_write_pin(AnalogPin.P13, 1023)
    # 値が大きいほど、速く動きます
    pins.analog_write_pin(AnalogPin.P14, 0)
    basic.pause(2000)
    pins.analog_write_pin(AnalogPin.P13, 0)
input.on_button_pressed(Button.A, on_button_pressed_a)

def on_button_pressed_b():
    pins.analog_write_pin(AnalogPin.P13, 0)
    pins.analog_write_pin(AnalogPin.P14, 1023)
    basic.pause(2000)
    pins.analog_write_pin(AnalogPin.P14, 0)
input.on_button_pressed(Button.B, on_button_pressed_b)

basic.show_icon(IconNames.HAPPY)