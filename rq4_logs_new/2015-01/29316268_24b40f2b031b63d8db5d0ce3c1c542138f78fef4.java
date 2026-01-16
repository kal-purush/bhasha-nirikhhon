package org.usfirst.frc.team1014.robot.commands.autonomous;

import org.usfirst.frc.team1014.robot.commands.CommandBase;

import edu.wpi.first.wpilibj.Utility;
import edu.wpi.first.wpilibj.command.Subsystem;

public class DriveStraightForward extends CommandBase {

	public long startTime;
	public long endTime;
	public long runTime;
	public long currentTime;
	
	public boolean hasStarted;
	
	public DriveStraightForward(double runTimeMicroSeconds)
	{
		requires((Subsystem) driveTrain);
		runTime = (long) runTimeMicroSeconds;
	}
	
	@Override
	protected void initialize() {
		
		hasStarted = false;
		currentTime = Utility.getFPGATime();		
	}

	@Override
	public String getConsoleIdentity() {
		
		return "DriveStraightForward";
	}

	@Override
	protected void execute() {
		
		if(!hasStarted)
		{
			startTime = Utility.getFPGATime();
			endTime = startTime + runTime;
			hasStarted = true;
		}
		currentTime = Utility.getFPGATime();
		driveTrain.mecanumDriveCartesian(0.0, 1.0, 0.0, 0.0);
	}

	@Override
	protected boolean isFinished() {
		
		if(currentTime > endTime)
			return true;
		return false;
	}

	@Override
	protected void end() {
		
		driveTrain.mecanumDriveCartesian(0, 0, 0, 0);
		
	}

	@Override
	protected void interrupted() {
		
		
	}

}