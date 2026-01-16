radio.set_group(69)
radio.set_transmit_power(7)
radio.set_transmit_serial_number(True)
my_serial = control.device_serial_number()
name="vote"
vote = 1

def on_button_pressed_a():
    global vote
    vote = (vote+1)%4    
    basic.show_number(vote)
input.on_button_pressed(Button.A, on_button_pressed_a)

def sending():
    radio.send_value("vote", my_serial)
input.on_button_pressed(Button.B, sending)