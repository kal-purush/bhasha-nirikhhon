class control():
    def __init__(self, scale, adjustment, running, list_movements, controller, clock, serial_port, serial_baudrate):
        self.scale = scale 
        self.adjustment = adjustment
        self.running = False
        self.list_movements = {
            abutton: 0,
            bbutton: 0,
            xbutton: 0,
            ybutton: 0,
            left_motor_right: 0,
            up_motor : 0,
            right_motor_right: 0,
            turning_amount: 0
        }
        
        #Initializes the joystick being used, which is the XBox Controller
        
        self.controller = pygame.joystick.Joystick(0)
        #Sets up the timing for the interface
        
        self.clock = pygame.time.Clock()
        
        self.serial_port = "dev/ttyUSB0"
        
        self.serial_baudrate = 9600
        
    ser = serial.Serial(port=serial_port, baudrate=serial_baudrate)    
    #Function that scales the movement on the joystick of the XBox Controller to the amount that needs to be moved by the robot
    
    def movement_scaler(self, raw, adjustment):
        if abs(raw) <= self.adjustment:
            return 0
        else:
            if raw > 0:
                return (raw-self.adjustment)/1.0
            elif raw < 0:
                return  (raw+self.adjustment)/1.0       
                
    #Creates variables that store the motor labels for the different motors used. Some motor values have also been put in for adjustments that may be made to add the claw          
    central_motor = 1
    right_motor = 2
    left_motor = 3
    up_quantity = 500
    right_quantity = 500
    left_quantity = 500
    
    #Correctly encodes the strings that are used to send quantities in order to send them to serial
    def printer(information):
        ser.write(information.encode('latin'))
        
    #Moves the robot upward by putting the central motor on higher output
    def move_up(quantity):
        printer("" + central_motor + "\n" + quantity + "\n")
        
    #Moves the robot leftward by putting the left motor on higher output
    def move_left(quantity):
        printer("" + left_motor + "\n" + quantity + "\n")
        
    #Moves the robot rightward by putting the right motor on higher output
    def move_right(quantity):
        printer("" + right_motor +  "\n" + quantity + "\n")
        
    #Moves the robot to the front by putting the right motor and the left motor on higher output
    def move_front(quantity):
        printer("" + right_motor + "\n" + quantity + "\n")
        printer("" + left_motor + "\n" + quantity + "\n")
    #Turns the robot in some direction with the power output provided    
    def move_turn(quantity):
        printer("" + right_motor + "\n" + quantity + "\n")
        printer("" + left_motor + "\n" + -1*quantity + "\n")
        
    #Starts controls and sets up event responses to control movements on the controller
    def controls_move(self):
        self.running = True
        while running:
            claw_movement = 99
            for event in pygame.event.get():
                if event.type == pygame.QUIT:
                    running = False
                #The buttons are used to control the leftward, rightward, and other motions of the claw. These have not been considered yet since the claw and its motor have not been mounted. That is why the code is commented out for now.
                #The joy axes are used to control the motion of the robot forward, leftward, rightward, and to allow the robot to turn as well. The axis labelled 0 is used for left to right motion and is located on the left joystick. The axis labelled 4 is used for up to right motion and is located on the right joystick.
                elif event.type == pygame.JOYAXISMOTION:
                    input = event.value
                    scaled_input = movement_scaler(input, 0.2)
                    right_quantity = scaled_input
                    if event.axis == 0:
                        if right_quantity<0:
                            move_left(right_quantity)
                            self.list_movement['left_motor_left'] = abs(scaled_input)
                        else:
                            move_right(right_quantity)
                            self.list_movement['left_motor_right'] = scaled_input
                    elif event.axis == 3:
                        move_turn(quantity)
                        self.list_movements['turning_amount'] = scaled_input        
                    elif event.axis == 4:
                        move_up(right_quantity)
                        self.list_movements['up_motor'] = scaled_input
    #Stops the controller by setting self.running to false                   
    def stop():
        self.running = False