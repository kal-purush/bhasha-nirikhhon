import pygame
import serial
pygame.init()
screen = pygame.display.set_mode((300,300))
running = True

ser = serial.begin("dev/cu.usbserial-1420", 9600)

# Test
if __name__ == '__main__':
    controller = control(100,0.1)
    controller.interface()
    try:
        controls_move()
        print("Controller is working")
    except KeyboardInterrupt:
        print("Stop")
    except:
        print("Unexpected error: ", sys.exc_infor()[0])
        raise
    finally:
        controller.stop()

class control():
    def __init__(self, scale, adjustment, running, list_movements, controller, clock):
        self.scale = scale 
        self.adjustment = adjustment
        self.running = False
        self.list_movements = {
            abutton: 0,
            bbutton: 0,
            xbutton: 0,
            ybutton: 0,
            left_motor_right: 0,
            up_motor = 0,
            right_motor_right: 0,
        }
        
        
        self.controller = pygame.joystick.Joystick(0)
        self.clock = pygame.time.Clock()

    def movement_scaler(self, raw, adjustment):
        if abs(raw) <= self.adjustment:
            return 0
        else:
            if raw > 0:
                return (raw-self.adjustment)/1.0
            elif raw < 0:
                return  (raw+self.adjustment)/1.0
    def controls_move(self):
        self.running = True
        while running:
            claw_movement = 99
            for event in pygame.event.get():
                if event.type == pygame.QUIT:
                    running = False
                elif event.type == pygame.JOYBUTTONDOWN:
                    input = self.get_button(event.button)

                    if event.button == 0:
                        ser.write("4")
                        ser.write(str(claw_movement))
                        
                        self.list_movements['abutton'] = claw_movement
                    elif event.button == 2:
                        ser.write("4")
                        ser.write(str(-1*claw_movement)))

                        self.list_movements['xbutton'] = -1*claw_movement
                    elif event.button == 1:
                        ser.write("4")
                        ser.write(str(claw_movement))
                        
                        self.list_movements['abutton'] = claw_movement
                    elif event.button == 3:
                        ser.write("4")
                        ser.write(str(-1*claw_movement)))
                        self.list_movements['xbutton'] = -1*claw_movement
                    
                elif event.type == pygame.JOYBUTTONUP:
                    if event.button == 1:
                        ser.write("4")
                        ser.write("0")
                        self.list_movements['bbutton'] = 0
                    elif event.button == 0:
                        ser.write("4")
                        ser.write("0")
                        self.list_movements['abutton'] = 0
                    elif event.button == 2:
                        ser.write("5")
                        ser.write("0")
                        self.list_movements['xbutton'] = 0
                    elif event.button == 3:
                        ser.write("5")
                        ser.write("0")
                        self.list_movements['ybutton'] = 0
                elif event.type == pygame.JOYAXISMOTION:
                    input = event.value
                    scaled_input = movement_scaler(input, 0.2)
                    if event.axis == 0:
                        ser.write("1")
                        ser.write(str(scaled_input))
                        self.list_movements['left_motor_right'] = scaled_input
                    elif event.axis == 3:
                        ser.write("2")
                        ser.write(str(scaled_input))
                        self.list_movements['right_motor_right'] = scaled_input
            elif event.type == pygame.JOYHATMOTION:
                input = event.value
                scaled_input = movement_scaler(input, 0.2)
                self.list_movements['up_motor'] = scaled_input
                if event.hat == 0:
                    ser.write("0")
                    ser.write(str(scaled_input))
    def stop():
        self.running = False