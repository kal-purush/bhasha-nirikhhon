package org.usfirst.frc.team6644.robot.commands;

import edu.wpi.first.wpilibj.command.Command;

import org.usfirst.frc.team6644.robot.Robot;
import org.usfirst.frc.team6644.robot.RobotPorts;

import edu.wpi.first.wpilibj.GenericHID.Hand;
import edu.wpi.first.wpilibj.GenericHID.RumbleType;
import edu.wpi.first.wpilibj.XboxController;

/**
 *
 */
public class DriveWithController extends Command {
	protected XboxController controller = new XboxController(RobotPorts.CONTROLLER.get());
	protected double left = 0;
	protected double right = 0;
	protected boolean rumble = false;
	protected boolean lastBValue = false;
	protected boolean currentBValue = false;
	protected boolean lastXValue = false;
	protected boolean currentXValue = false;
	protected boolean disableMotors = false;
	protected double movementX;
	protected double movementY;

	public DriveWithController() {
		// Use requires() here to declare subsystem dependencies
		requires(Robot.drivemotors);
	}

	// Called just before this Command runs the first time
	protected void initialize() {
	}

	// Called repeatedly when this Command is scheduled to run
	protected void execute() {
		calculateMotorOutputs();
		/*
		 * if (controller.getBButton()) { controller.setRumble(RumbleType.kLeftRumble,
		 * 1); controller.setRumble(RumbleType.kRightRumble, 1); rumbledumble = true; }
		 * else { controller.setRumble(RumbleType.kLeftRumble, 0);
		 * controller.setRumble(RumbleType.kRightRumble, 0); rumbledumble = false; }
		 */
		currentBValue = controller.getBButton();
		if (lastBValue != currentBValue) {
			lastBValue = currentBValue;
			if (currentBValue) {
				rumble = !rumble;
			}
		}

		currentXValue = controller.getBumper(Hand.kRight);
		if (lastXValue != currentXValue) {
			lastXValue = currentXValue;
			if (currentXValue) {
				disableMotors = !disableMotors;
			}
		}

		if (rumble) {
			setRumble(1, 1);
		} else {
			setRumble(0, 0);
		}
		Robot.drivemotors.arcadeDrive(movementY, movementX);

	}

	protected void setRumble(double right, double left) {
		controller.setRumble(RumbleType.kLeftRumble, left);
		controller.setRumble(RumbleType.kRightRumble, right);
	}

	// Make this return true when this Command no longer needs to run execute()
	protected boolean isFinished() {
		return false;
	}

	// Called once after isFinished returns true
	protected void end() {
		setRumble(0, 0);
	}

	protected void calculateMotorOutputs() {
		movementX = controller.getRawAxis(0) * -1;
		movementY = controller.getRawAxis(1) * -1;
		if (disableMotors) {
			movementX = 0;
			movementY = 0;
		}
	}

	// Called when another command which requires one or more of the same
	// subsystems is scheduled to run
	protected void interrupted() {
	}
}