start = 0
end = 0
false_start = False
running = False

def on_pin_pressed_p0():
    global running, false_start, start
    basic.show_number(3)
    basic.show_number(2)
    basic.show_number(1)
    basic.clear_screen()
    running = False
    false_start = False
    basic.pause(1000 + randint(0, 2000))
    if not (false_start):
        start = input.running_time()
        running = True
        led.stop_animation()
        basic.clear_screen()
        led.plot(randint(0, 4), randint(0, 4))
input.on_pin_pressed(TouchPin.P0, on_pin_pressed_p0)

def on_pin_pressed_p1():
    global running, end, false_start
    if running:
        running = False
        end = input.running_time()
        basic.show_leds("""
            # # . . .
                        # # . . .
                        # # . . .
                        # # . . .
                        # # . . .
        """)
        basic.pause(1000)
        basic.show_number(end - start)
    else:
        false_start = True
        basic.show_leds("""
            . . . . .
                        # . # . .
                        . # . . .
                        # . # . .
                        . . . . .
        """)
input.on_pin_pressed(TouchPin.P1, on_pin_pressed_p1)

def on_pin_pressed_p2():
    global running, end, false_start
    if running:
        running = False
        end = input.running_time()
        basic.show_leds("""
            . . . # #
                        . . . # #
                        . . . # #
                        . . . # #
                        . . . # #
        """)
        basic.pause(1000)
        basic.show_number(end - start)
    else:
        false_start = True
        basic.show_leds("""
            . . . . .
                        . . # . #
                        . . . # .
                        . . # . #
                        . . . . .
        """)
input.on_pin_pressed(TouchPin.P2, on_pin_pressed_p2)