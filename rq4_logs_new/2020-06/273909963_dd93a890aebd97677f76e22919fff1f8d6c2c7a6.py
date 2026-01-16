def Startposition():
    basic.pause(BlTouchDELAY)
    pins.servo_write_pin(AnalogPin.P0, BlTouchReset)
    basic.pause(BlTouchDELAY)
    pins.servo_write_pin(AnalogPin.P0, BlTouchStow)
    basic.pause(BlTouchDELAY)
def ShowNormal():
    serial.write_line("")
    basic.show_leds("""
        . . . . .
        . . . . .
        . . . . .
        . . . . .
        # # # # #
        """)
def CheckReadyState():
    if pins.digital_read_pin(DigitalPin.P1) == 1:
        Startposition()
        Deploy()
        serial.write_line("")
        serial.write_line("Bl-Touch error, trying to recover")
        if pins.digital_read_pin(DigitalPin.P1) == 1:
            serial.write_line("")
            serial.write_line("ERROR can not recover Bl-Touch device")
            serial.write_line("Please Hard Reset the device")
            serial.write_line("Program STOPPED")
            basic.show_icon(IconNames.SAD)
            while True:
                pass
def ShowError():
    serial.write_string(" ERROR_")
    serial.write_number(Count_Error)
    basic.show_icon(IconNames.NO)
    basic.pause(5000)

def on_button_pressed_a():
    global StatusRUNSTOP, StatusSWMODEONOFF
    StatusRUNSTOP = 0
    if StatusSWMODEONOFF == 0:
        StatusSWMODEONOFF = 1
        basic.show_string("SW mode")
    else:
        StatusSWMODEONOFF = 0
        basic.show_string("HW mode")
    basic.pause(5000)
    Startposition()
input.on_button_pressed(Button.A, on_button_pressed_a)

def Deploy():
    pins.servo_write_pin(AnalogPin.P0, BlTouchDeploy)
    basic.show_arrow(ArrowNames.EAST)
    basic.pause(BlTouchDELAY)
def ShowOk():
    serial.write_string(" OK")
    basic.show_icon(IconNames.YES)
    basic.pause(1000)

def on_button_pressed_ab():
    global StatusRUNSTOP
    StatusRUNSTOP = 0
    pins.servo_write_pin(AnalogPin.P0, BlTouchReset)
    basic.show_icon(IconNames.SMALL_HEART)
    Startposition()
    basic.show_icon(IconNames.HEART)
    basic.pause(2000)
    basic.clear_screen()
input.on_button_pressed(Button.AB, on_button_pressed_ab)

def SwmodeOn():
    pins.servo_write_pin(AnalogPin.P0, BltouchSWMODE)
    basic.pause(BlTouchDELAY)

def on_button_pressed_b():
    global StatusRUNSTOP
    if StatusRUNSTOP == 1:
        StatusRUNSTOP = 0
    else:
        StatusRUNSTOP = 1
input.on_button_pressed(Button.B, on_button_pressed_b)

def WriteLog(Log_Line: number, Log_Angle: number):
    serial.write_string("#")
    serial.write_number(Log_Line)
    serial.write_string(" Servo angle: ")
    serial.write_number(Log_Angle)
OUTPUT = 0
ERROR = 0
Count = 0
Count_Error = 0
BltouchSWMODE = 0
BlTouchStow = 0
BlTouchDeploy = 0
BlTouchReset = 0
StatusRUNSTOP = 0
BlTouchDELAY = 0
StatusSWMODEONOFF = 0
StatusDEPLOYSTOW = 0
StatusSWMODEONOFF = 1
BlTouchDELAY = 350
StatusRUNSTOP = 0
BlTouchReset = 160
BlTouchDeploy = 10
BlTouchStow = 90
BltouchSWMODE = 60
MaxAnlge = 300
servocenterposition = 1450
ServoDELAY = 500
pins.set_pull(DigitalPin.P1, PinPullMode.PULL_UP)
pins.servo_set_pulse(AnalogPin.P2, servocenterposition)
basic.pause(ServoDELAY)
Startposition()
serial.redirect_to_usb()
basic.show_leds("""
    . . . . .
    . . . . .
    . . . . .
    . . . . .
    # # . # #
    """)
Count_Error = 0

def on_forever():
    global Count, ERROR, OUTPUT, Count_Error
    if StatusRUNSTOP == 1:
        ShowNormal()
        Count += 1
        Deploy()
        CheckReadyState()
        if StatusSWMODEONOFF == 1:
            SwmodeOn()
        ERROR = 1
        ServoAngle = 0
        while ServoAngle <= MaxAnlge:
            pins.servo_set_pulse(AnalogPin.P2, servocenterposition - ServoAngle)
            basic.pause(5)
            OUTPUT = ServoAngle
            if pins.digital_read_pin(DigitalPin.P1) == 1:
                ERROR = 0
                break
            ServoAngle += 1
        pins.servo_set_pulse(AnalogPin.P2, servocenterposition)
        basic.pause(ServoDELAY)
        WriteLog(Count, OUTPUT)
        if ERROR == 1:
            Count_Error += 1
            ShowError()
        else:
            ShowOk()
basic.forever(on_forever)