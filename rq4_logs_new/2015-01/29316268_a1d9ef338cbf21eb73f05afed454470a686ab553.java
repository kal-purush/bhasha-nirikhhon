package org.usfirst.frc.team1014.robot.commands;

import edu.wpi.first.wpilibj.command.Subsystem;

public class DriveAndGrab extends CommandBase {

	public double driveTime = 1000000; // in microseconds
	public double grabTime = 1000000; // also in microseconds
	
	public DriveAndGrab()
	{
		requires((Subsystem) driveTrain);
		requires((Subsystem) grabber);
	}
	
	@Override
	protected void initialize() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String getConsoleIdentity() {
		// TODO Auto-generated method stub
		return "DriveAndGrab";
	}

	@Override
	protected void execute() {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected boolean isFinished() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	protected void end() {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected void interrupted() {
		// TODO Auto-generated method stub
		
	}

}