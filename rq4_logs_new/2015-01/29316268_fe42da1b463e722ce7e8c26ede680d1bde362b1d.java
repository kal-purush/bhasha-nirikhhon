package org.usfirst.frc.team1014.robot.commands.autonomous;

import org.usfirst.frc.team1014.robot.commands.CommandBase;

import edu.wpi.first.wpilibj.command.Subsystem;

public class AutoTurn extends CommandBase {

	boolean hasFinished;
	double currentGyro;
	double angleToMoveThrough;
	double finishAngle;
	boolean direction; // true for right, false for left
	
	public AutoTurn(double angle, boolean direc)
	{
		requires((Subsystem) driveTrain);
		requires((Subsystem) mxp);
		angleToMoveThrough = angle;
		direction = direc;
	}
	
	@Override
	protected void end() {
		// TODO Auto-generated method stub
		driveTrain.mecanumDriveCartesian(0, 0, 0, 0);
	}

	@Override
	protected void execute() {
		// TODO Auto-generated method stub
		if(mxp.getAngle() < 0)
			currentGyro = mxp.getAngle() + 360;
		else
			currentGyro = mxp.getAngle();
		
		if(mxp.getAngle() < finishAngle && direction)
		{
			driveTrain.mecanumDriveCartesian(0.0, 0.0, .20, 0);
		}
		else if(mxp.getAngle() > finishAngle && !direction)
		{
			driveTrain.mecanumDriveCartesian(0.0, 0.0, -.20, 0);
		}
		
	}

	@Override
	protected void initialize() {
		// TODO Auto-generated method stub
		hasFinished = false;
		
		if(mxp.getAngle() < 0)
			currentGyro = mxp.getAngle() + 360;
		else
			currentGyro = mxp.getAngle();
		
		finishAngle = currentGyro + angleToMoveThrough;
	}

	@Override
	protected void interrupted() {
		// TODO Auto-generated method stub

	}

	@Override
	protected boolean isFinished() {
		// TODO Auto-generated method stub
		if(currentGyro == finishAngle)
			return true;
		return false;
	}

	@Override
	public String getConsoleIdentity() {
		// TODO Auto-generated method stub
		return "AutoTurn";
	}

}