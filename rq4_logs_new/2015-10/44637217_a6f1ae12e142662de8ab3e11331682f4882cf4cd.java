import edu.wpi.first.wpilibj.command.Command;

import org.usfirst.frc.team166.robot.Robot;

/**
 *
 */
public class TurnProportional extends Command {

	public TurnProportional(double desiredAngle) {
		// Use requires() here to declare subsystem dependencies
		requires(Robot.drive);

	}

	// Called just before this Command runs the first time
	@Override
	protected void initialize() {
		Robot.drive.GyroReset();
	}

	// Called repeatedly when this Command is scheduled to run
	@Override
	protected void execute() {
		Robot.drive.turnProportional(desiredAngle);
	}

	// Make this return true when this Command no longer needs to run execute()
	@Override
	protected boolean isFinished() {
		return ((Math.abs(Math.abs(Robot.drive.gyro.getAngle()) - Math.abs(desiredAngle))) <= 5);
	}

	// Called once after isFinished returns true
	@Override
	protected void end() {
		Robot.drive.stopMotors();
	}

	// Called when another command which requires one or more of the same
	// subsystems is scheduled to run
	@Override
	protected void interrupted() {
	}
}