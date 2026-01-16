
package org.usfirst.frc.team1757.robot;

//Class imports from the WPILibj library
import edu.wpi.first.wpilibj.IterativeRobot;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import edu.wpi.first.wpilibj.CANTalon;
import edu.wpi.first.wpilibj.DriverStation;
import edu.wpi.first.wpilibj.Joystick;
import edu.wpi.first.wpilibj.Solenoid;
import edu.wpi.first.wpilibj.DoubleSolenoid;
import edu.wpi.first.wpilibj.smartdashboard.SmartDashboard;
import edu.wpi.first.wpilibj.Relay;
import edu.wpi.first.wpilibj.SpeedController;

/**
 * The VM is configured to automatically run this class, and to call the
 * functions corresponding to each mode, as described in the IterativeRobot
 * documentation. If you change the name of this class or the package after
 * creating this project, you must also update the manifest file in the resource
 * directory.
 */

public class Robot extends IterativeRobot {
    /**
     * This function is run when the robot is first started up and should be
     * used for any initialization code.
     */
	
	// Constant variables for the button codes on the F310 gamepad
	public static final int 
	BUTTON_A = 1, BUTTON_B = 2, BUTTON_X = 3,
	BUTTON_Y = 4, BUTTON_LB = 5, BUTTON_RB = 6,
	BUTTON_BACK = 7, BUTTON_START = 8, BUTTON_LS = 9, BUTTON_RS = 10;
	
	// Constant variable for the location of the button layout file
	public static final String FILEPATH = "/RobotButtonLayout.txt";
	// Array variable for button function for use with gamepad button layout
	public String[] BUTTONFUNCTION = null;
	
	
	/* Analog (raw)Axis codes
	Left Stick Up/Dn = 2 Axis (Dn+)
	Left Stick L/R = Axis 1 (L-)
	Right Stick Up/Dn = Axis 5 (Dn+)
	Right Stick L/R = Axis 4 (L-)
	Left Trigger = Axis 3 (+)
	Right Trigger = Axis 3 (-)
	Gamepad Up = Axis 2(-)
	Gamepad Dn = Axis 2(+)
	Gamepad L = Axis 6(-)
	Gamepad R = Axis 6(+)

	NOTE: Mode switch changes Axis 2 between Left Stick Up/Dn and Gamepad Up/Dn
	*/
	
	//Class reference variables
		//I don't understand why we use these, you generally aren't supposed to have null declarations. Safer?
		//See SpeedController instantiations for alternative examples
	CANTalon talon;
	Joystick gamepad;
	Solenoid singleSolenoid;
	SpeedController speedController;
	Relay spikeRelay;
	DoubleSolenoid doubleSolenoid;
	
	
	//void function designed to read all 
	public String[] readLines(String path)
	{
		try {
    		Scanner filescan = new Scanner(path);
    		 List<String> outList = Arrays.asList(); //Safe empty declaration
    		while (filescan.hasNextLine())
    		{
    			outList.add(filescan.nextLine());
    		}
    		filescan.close();
    		return (String[]) outList.toArray(); // Possibly dangerous type casting
    	}
    	catch (Exception e)
    	{
    		System.out.println("Error opening button layout file, will use defaults\n" + e.toString());
    		return new String[] {};
    	}
	}
	
    public void robotInit() {
    	//Object instantiation and definition for dependencies
    	
    	//The port numbers for these variables are not correct. Assure that proper values are entered prior to use.
    	
    	//Talon object instantiation on CAN port 0
    	//speedController = new CANTalon(0);
    	SpeedController frontRight = new CANTalon(0);
    	//Talon object instantiation on CAN port 1
    	SpeedController frontLeft = new CANTalon(1);
    	//Talon object instantiation on CAN port 2
    	SpeedController backRight = new CANTalon(2);
    	//Talon object instantiation on CAN port 3
    	SpeedController backRleft = new CANTalon(3);
    	//Talon object instantiation on CAN port 4
    	SpeedController heightAdjust = new CANTalon(4);
    	
    	//Joystick object instantiation on port 0; used for robot operation
    	gamepad = new Joystick(0);
    	
    	//spikeRelay object instantiation on PWM port 0; used for sprinkler valve
    	spikeRelay = new Relay(0);
    	//Relay spikeRelay = new Relay(0);
    		//Note that we will likely be using more than one spikeRelay in practice; append to reflect hardware change
    		//Incomplete: append for compressor controls
    	
    	//Double solenoid object instantiation on PWM port 1,2; used for piston control
    	DoubleSolenoid pistonControl = new DoubleSolenoid(1,2); // These port numbers are likely incorrect
    	//Append solenoid implementation to reflect hardware changes. 
    	//Likely will include second solenoid for compressor air input control.
    	
    	
    	//Load the button layout from SmartDashboard/ .txt File. Note: With albeit limited experimentation, SmartDashboard is not reliable
    	BUTTONFUNCTION = readLines(FILEPATH);
    }

    /**
     * This function is called periodically during autonomous
     */
    public void autonomousPeriodic() {

    }

    /**
     * This function is called periodically during operator control
     */
    public void teleopPeriodic() {
    	//Loop during FRC safe operator control time
    	
    	/*
    	 * In this branch of the project support for a loop & if maze based system of controller input; however,
    	 * future branches should utilize event based input for the sake of a cleaner project and better programming practice.
    	 * 
    	 * 
    	 * */
        while(isEnabled() && isOperatorControl()){
        	//Example button input
        	if (gamepad.getRawButton(BUTTON_A)){
        		spikeRelay.set(Relay.Value.kForward);
        		//Example function
        	}
        		
        }
    }
    
    /**
     * This function is called periodically during test mode
     */
    public void testPeriodic() {
    
    }
    
}