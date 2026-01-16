package org.usfirst.frc.team4043.robot;

import edu.wpi.first.wpilibj.Joystick;
import edu.wpi.first.wpilibj.buttons.Button;
import edu.wpi.first.wpilibj.buttons.JoystickButton;

import org.usfirst.frc.team4043.robot.commands.ClampJOD;
import org.usfirst.frc.team4043.robot.commands.ExampleCommand;
import org.usfirst.frc.team4043.robot.commands.MoveJODDown;
import org.usfirst.frc.team4043.robot.commands.MoveJODUp;
import org.usfirst.frc.team4043.robot.commands.Shift;
import org.usfirst.frc.team4043.robot.commands.UnShift;
import org.usfirst.frc.team4043.robot.commands.UnclampJOD;

/**
 * This class is the glue that binds the controls on the physical operator
 * interface to the commands and command groups that allow control of the robot.
 */
public class OI {
	//// CREATING BUTTONS
	// One type of button is a joystick button which is any button on a
	//// joystick.
	// You create one by telling it which joystick it's on and which button
	// number it is.
	// Joystick stick = new Joystick(port);
	// Button button = new JoystickButton(stick, buttonNumber);

	// There are a few additional built in buttons you can use. Additionally,
	// by subclassing Button you can create custom triggers and bind those to
	// commands the same as any other Button.

	//// TRIGGERING COMMANDS WITH BUTTONS
	// Once you have a button, it's trivial to bind it to a button in one of
	// three ways:

	// Start the command when the button is pressed and let it run the command
	// until it is finished as determined by it's isFinished method.
	// button.whenPressed(new ExampleCommand());

	// Run the command while the button is being held down and interrupt it once
	// the button is released.
	// button.whileHeld(new ExampleCommand());

	// Start the command when the button is released and let it run the command
	// until it is finished as determined by it's isFinished method.
	// button.whenReleased(new ExampleCommand());
	
	public Joystick driveStick = new Joystick(0);
	public Button shifter = new JoystickButton(driveStick, 6);
	
	public Button armUp = new JoystickButton(driveStick, 1);
	public Button armDown = new JoystickButton(driveStick, 2);
	
	public Button clamp = new JoystickButton(driveStick, 3);
	public Button unClamp = new JoystickButton(driveStick, 4);
	
	public Button stupidShifterOfMeira = new JoystickButton(driveStick, 5);
	
	public OI() {
		shifter.whenPressed(new Shift());
		shifter.whenReleased(new UnShift());
		//stupidShifterOfMeira.whenPressed(new UnShift());
		
		armUp.whenPressed(new MoveJODUp());
		armDown.whenPressed(new MoveJODDown());
		
		clamp.whenPressed(new ClampJOD());
		unClamp.whenPressed(new UnclampJOD());
	}
	
	public Joystick getDriveStick() {
		return driveStick;
	}
}