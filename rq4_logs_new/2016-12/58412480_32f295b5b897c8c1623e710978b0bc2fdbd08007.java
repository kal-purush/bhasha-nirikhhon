package org.usfirst.frc.team303.robot;

public class ActionDriveEncodersSupreme implements Action{
	double encoders;
	double power;
	double encoderStart;
	double navXStart;
	double tuningConstant;
	boolean firstRun;
	double previousEncoders;
	double lateralError;
	double initalDisplacement;
	double extraDistance;//calculates difference between straight line distance and arc traveled distance
	public ActionDriveEncodersSupreme(double encodersC, double powerC,double tuningConstantC){
		encoders=encodersC;//how many encoder ticks
		power=powerC;//how fast
		tuningConstant=tuningConstantC;
		firstRun=true;
	}
	public boolean isFinished(){
		if(firstRun)
			return false;
		return Math.abs((((Robot.drivebase.rDriveEnc.getDistance()+Robot.drivebase.lDriveEnc.getDistance())/2)+extraDistance)-(encoderStart))>=encoders;
	}
	public void run(){
		if(firstRun){
			encoderStart = (Robot.drivebase.rDriveEnc.getDistance()+Robot.drivebase.lDriveEnc.getDistance())/2;
			Robot.drivebase.navX.zeroYaw();
			firstRun = false;
			lateralError=0;
			previousEncoders=encoderStart;
			initalDisplacement=Robot.drivebase.navX.getDisplacementY();
		}
		else {
			double averageEncoders= (Robot.drivebase.rDriveEnc.getDistance()+Robot.drivebase.lDriveEnc.getDistance())/2;
			lateralError += (averageEncoders-previousEncoders)*Math.sin(Robot.drivebase.navX.getYaw());
			lateralError+= Robot.drivebase.navX.getDisplacementY()-initalDisplacement;
			extraDistance+=Math.abs(((averageEncoders-previousEncoders)*Math.sin(Robot.drivebase.navX.getYaw()))+(Robot.drivebase.navX.getDisplacementY()-initalDisplacement));
			Robot.drivebase.drive(-1*(power+(lateralError*tuningConstant)),power-(lateralError*tuningConstant));
			previousEncoders=averageEncoders;
		}
	}
}