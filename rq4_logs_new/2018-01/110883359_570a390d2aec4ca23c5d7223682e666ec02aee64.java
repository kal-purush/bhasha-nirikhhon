package org.usfirst.frc.team4277.robot;

public interface PortMap {
	
	
		//  PWM Ports
	// ex. public final static int JOE_DOE_SENSOR= 13;
	
		// CAN
	// ex. public final static int JOE_DOE_SENSOR= 13;
		
		//  Digital
	// ex. public final static int JOE_DOE_SENSOR= 13;
	
		//  Analog
	// ex. public final static int JOE_DOE_SENSOR= 13;
	public final static int TILTER_ANALOG = 3;
	public final static int Analoginput = 2;
	
		// Laptop USB Ports
	// ex. public final static int JOE_DOE_SENSOR= 13;
	public final int LEFT_JOYSTICK = 0;
	public final int RIGHT_JOYSTICK = 5;
	public final int XBOX_CONTROLLER = 3;
	
		// XBoxController
	// ex. public final static int JOE_DOE_SENSOR= 13;
	public final int XBOX_LEFT_STICK_AXIS_Y = 1;
	public final int XBOX_LEFT_STICK_AXIS_X = 2;
	public final int XBOX_TRIGGER = 3;
	public final int XBOX_RIGHT_STICK_AXIS_X = 4;
	public final int XBOX_RIGHT_STICK_AXIS_Y = 5;
	
	public final int XBOX_BUTTON_A = 1;
	public final int XBOX_BUTTON_B = 2;
	public final int XBOX_BUTTON_X = 3;
	public final int XBOX_BUTTON_Y = 4;
	public final int XBOX_BUTTON_LB = 5;
	public final int XBOX_BUTTON_RB = 6;
	public final int XBOX_BUTTON_BACK = 7;
	public final int XBOX_BUTTON_START = 8;
	public final int XBOX_JOY_LEFT_BUTTON = 9;
	public final int XBOX_JOY_RIGHT_BUTTON = 10;
		
//	The buttons on the xbox controller follow this mapping
//
//	1: A
//	2: B
//	3: X
//	4: Y
//	5: Left Bumper
//	6: Right Bumper
//	7: Back
//	8: Start
//	9: Left Joystick
//	10: Right Joystick
//
//	The axis on the controller follow this mapping
//	(all output is between -1 and 1)
//	1: Left Stick X Axis
//	-Left:Negative ; Right: Positive
//	2: Left Stick Y Axis
//	-Up: Negative ; Down: Positive
//	3: Triggers
//	-Left: Positive ; Right: Negative
//	4: Right Stick X Axis
//	-Left: Negative ; Right: Positive
//	5: Right Stick Y Axis
//	-Up: Negative ; Down: Positive
//	6: Directional Pad (Not recommended, buggy)

}