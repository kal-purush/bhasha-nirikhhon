package org.usfirst.frc.team1014.robot.subsystems;

import org.usfirst.frc.team1014.robot.RobotMap;
import org.usfirst.frc.team1014.robot.commands.DpadDrive;

import edu.wpi.first.wpilibj.Gyro;
import edu.wpi.first.wpilibj.RobotDrive;
import edu.wpi.first.wpilibj.SpeedController;
import edu.wpi.first.wpilibj.Talon;
import edu.wpi.first.wpilibj.Ultrasonic;

public class TankDriveTrain extends BadSubsystem {
	private static TankDriveTrain instance;
	
	RobotDrive train;
	SpeedController frontLeft, backLeft, frontRight, backRight;
	Ultrasonic backUltrasonic;
	
    public static TankDriveTrain getInstance()
    {
        if (instance == null)
        {
            instance = new TankDriveTrain();
        }
        return instance;
    }
	
    private TankDriveTrain()
    {
    	
    }
    
    @Override
	protected void initialize()
    {
        frontLeft = new Talon(RobotMap.frontLeftController);
        backLeft = new Talon(RobotMap.backLeftController);
        frontRight = new Talon(RobotMap.frontRightController);
        backRight = new Talon(RobotMap.backRightController); 

        backUltrasonic = new Ultrasonic(RobotMap.backUltrasonicPing, RobotMap.backUltrasonicEcho);
        backUltrasonic.setEnabled(true);
        backUltrasonic.setAutomaticMode(false);
        
    	train = new RobotDrive(frontLeft, backLeft, frontRight, backRight);
    	
    	//train.setInvertedMotor(RobotDrive.MotorType.kFrontRight, true); 
    	//train.setInvertedMotor(RobotDrive.MotorType.kFrontLeft, true);
    	
	}

	@Override
	public String getConsoleIdentity() 
	{
		return "MikeDriveTrain";
	}

	@Override
	protected void initDefaultCommand() 
	{
		this.setDefaultCommand(new DpadDrive()); 
	}
	
    public void tankDrive(double leftY, double rightY) //analogs
    {
        train.tankDrive(leftY, rightY);
    }
    public void dpadDrive(double leftX, double leftY, double rightX)
    {
    	if((Math.abs(leftX)+Math.abs(leftY)) > 
		(Math.abs(rightX)*2))// if left stick is being used more than the right, this works 
	{
		
		if(Math.abs(leftX) < Math.abs(leftY)) // if more Y than X
		{
			frontLeft.set(-(leftY));// move forward/back
			frontRight.set((leftY));
			backLeft.set(-(leftY));
			backRight.set((leftY));
		}
		else
		{
			frontLeft.set(-(leftX));  // strafe works
			frontRight.set(-(leftX));
			backLeft.set(leftX);
			backRight.set(leftX);
		}
	}
	else
	{
		frontLeft.set(rightX); // rotate robot 
		frontRight.set(rightX);
		backLeft.set(rightX);
		backRight.set(rightX);
	}
    	
    	
    }
    public double getDistanceToWall()
    {
        double dist = backUltrasonic.getRangeMM();
        
        //backUltrasonic.ping();
        
        return dist;
    }
    public void mecanumDriveCartesian(double leftX, double leftY, double rightX, double gyro) 
    {
    	if((Math.abs(leftX)+Math.abs(leftY)) > (Math.abs(rightX)*2))// if left stick is being used more than the right, this works
    	{
        	train.mecanumDrive_Cartesian(leftX, leftY, rightX, gyro);
    	}
    	else
    	{
    		frontLeft.set(rightX); // rotate robot 
    		frontRight.set(rightX);
    		backLeft.set(rightX);
    		backRight.set(rightX);
    	}
    }
    
    public void setMotors(double fl, double bl, double fr, double br)
    {
    	frontLeft.set(fl);
    	backLeft.set(bl);
    	frontRight.set(fr);
    	backRight.set(br);
    }
    public Ultrasonic getBackUltrasonic()
    {
    	return backUltrasonic;
    }
    public double getBackUltrasonicDistanceMM()
    {
    	if(backUltrasonic.isRangeValid())
    	{
    		return backUltrasonic.getRangeMM();
    	}
    	return 0.0;
    }
	public static void so(Object so)
	{
		System.out.println("MikeDriveTainr: " + so);
	}
}