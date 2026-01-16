def on_button_pressed_a():
    global Variable_crono
    Variable_crono += 1
input.on_button_pressed(Button.A, on_button_pressed_a)

def on_gesture_shake():
    global Variable_passos
    Variable_passos += 1
    basic.show_string("" + str((Variable_passos)))
input.on_gesture(Gesture.SHAKE, on_gesture_shake)

def on_button_pressed_ab():
    global Variable_passos, Variable_Temps
    Variable_passos = 0
    Variable_Temps = 0
    basic.clear_screen()
input.on_button_pressed(Button.AB, on_button_pressed_ab)

def on_button_pressed_b():
    global Variable_passos
    Variable_passos = 0
    Variable_passos = 0
    basic.show_string("" + str((Variable_Temps)))
    basic.pause(1000)
    basic.clear_screen()
input.on_button_pressed(Button.B, on_button_pressed_b)

Variable_Temps = 0
Variable_passos = 0
Variable_passos = 0
Variable_Temps = 0
Variable_crono = 0
if Variable_crono == 1:
    Variable_Temps += 1
    basic.pause(1000)

def on_forever():
    pass
basic.forever(on_forever)