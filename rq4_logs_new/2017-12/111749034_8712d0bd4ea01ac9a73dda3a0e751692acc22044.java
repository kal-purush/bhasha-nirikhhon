package org.usfirst.frc.team6644.robot.commands;

import org.usfirst.frc.team6644.robot.RobotPorts;

import edu.wpi.first.wpilibj.XboxController;
import edu.wpi.first.wpilibj.GenericHID.Hand;
import edu.wpi.first.wpilibj.GenericHID.RumbleType;
import edu.wpi.first.wpilibj.command.Command;

/**
 *
 */
public class DriveWithDualInput extends DriveWithJoystickWithSensitivity {
	protected boolean lastXValue = false;
	protected boolean currentXValue = false;
	protected boolean disableMotors = false;
	protected XboxController controller = new XboxController(RobotPorts.CONTROLLER.get());
	protected boolean rumble = false;
	protected boolean lastBValue = false;
	protected boolean currentBValue = false;

	public DriveWithDualInput() {
		// Use requires() here to declare subsystem dependencies
		// requires();
		super();
	}

	// Called just before this Command runs the first time
	// protected void initialize() {
	// }
	@Override
	// Called repeatedly when this Command is scheduled to run
	protected void execute() {
		super.execute();

		currentXValue = controller.getBumper(Hand.kRight);
		if (lastXValue != currentXValue) {
			lastXValue = currentXValue;
			if (currentXValue) {
				disableMotors = !disableMotors;
			}
		}

		currentBValue = joystick.getRawButton(2);
		if (lastBValue != currentBValue) {
			lastBValue = currentBValue;
			if (currentBValue) {
				rumble = !rumble;
			}
		}
		if (rumble) {
			setRumble(1, 1);
		} else {
			setRumble(0, 0);
		}

	}

	protected void setRumble(double right, double left) {
		controller.setRumble(RumbleType.kLeftRumble, left);
		controller.setRumble(RumbleType.kRightRumble, right);
	}

	protected void calculateMotorOutputs() {
		super.calculateMotorOutputs();
		if (disableMotors) {
			left = 0;
			right = 0;
		}
	}

	// Make this return true when this Command no longer needs to run execute()
	// protected boolean isFinished() {
	// return false;
	// }

	// Called once after isFinished returns true
	// protected void end() {
	// }

	// Called when another command which requires one or more of the same
	// subsystems is scheduled to run
	// protected void interrupted() {
	// }
}