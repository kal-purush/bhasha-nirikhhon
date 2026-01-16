package org.usfirst.frc.team997.robot.subsystems;

import java.util.Timer;
import java.util.TimerTask;

import edu.wpi.first.wpilibj.smartdashboard.SmartDashboard;

public class AccelMotor {
	
	private VelMotor mMotor;
	private double mMaxAccel;
	private double desiredVel;
	private double accelCappedVel;
	public AccelMotor(VelMotor motor, double maxAccelPerSecond) {
		mMotor = motor;
		mMaxAccel = (maxAccelPerSecond/200); 
		desiredVel = 0;
    	accelCappedVel = 0;
    	mMotor.setDesiredVelocity(0);
	}
	public void start() {
    	try{
    		Timer myTimer = new Timer();
    		myTimer.schedule(update, 0, 5);
    	} catch (Exception a) {
    		
    	}
		mMotor.start();
	}
	private void update() {
	    	if((Math.abs(accelCappedVel-desiredVel)<mMaxAccel) || (mMaxAccel == 0)) {
	    		accelCappedVel = desiredVel;
	    	} else if (desiredVel<accelCappedVel){
	    		accelCappedVel -= mMaxAccel;	
	    	} else if (desiredVel>accelCappedVel) {
	    		accelCappedVel += mMaxAccel;
	    	}
	    mMotor.setDesiredVelocity(accelCappedVel);
	}
	public void setDesiredVelocity(double a) {
		   desiredVel = a;
		   SmartDashboard.putNumber("accelCappedVel", accelCappedVel);
		   SmartDashboard.putNumber("desiredVel(accel)", desiredVel);
		   SmartDashboard.putNumber("Max Accel", mMaxAccel);
	}
	    
	private TimerTask update = new TimerTask() {
			
			@Override
			public void run() {
				update();
			}
	};
	
	public void setMaxAccel(double a) {
		mMaxAccel = a; 
	}
}
