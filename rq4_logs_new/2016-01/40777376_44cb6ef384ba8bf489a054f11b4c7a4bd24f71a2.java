package org.usfirst.frc4904.robot.subsystems.chassis;

import org.usfirst.frc4904.standard.subsystems.chassis.SolenoidShifters;
import org.usfirst.frc4904.standard.subsystems.chassis.TankDriveShifting;
import org.usfirst.frc4904.standard.subsystems.motor.Motor;

import edu.wpi.first.wpilibj.PIDController;
import edu.wpi.first.wpilibj.PIDOutput;
import edu.wpi.first.wpilibj.PIDSource;

public class TankDriveShiftingPID extends TankDriveShifting implements PIDOutput{
	
	private PIDController pid;
	private double ySpeed;     

	public TankDriveShiftingPID(String name, Motor leftWheelA, Motor leftWheelB, Motor rightWheelA, Motor rightWheelB,
			SolenoidShifters shifter, double Kp, double Ki, double Kd, PIDSource source) {
		super(name, leftWheelA, leftWheelB, rightWheelA, rightWheelB, shifter);
		pid = new PIDController(Kp, Ki, Kd, source, this);
		ySpeed = 0;
		pid.enable();
		pid.setInputRange(-180.0f,  180.0f);
	    pid.setOutputRange(-1.0, 1.0);
	    pid.setContinuous(true);
	}
	@Override
	public void move2dc(double xSpeed, double ySpeed, double turnSpeed) {
		this.ySpeed = ySpeed;
		pid.setSetpoint(turnSpeed);
		
	}
	@Override
	public void pidWrite(double turnSpeed) {
		move2dp(ySpeed, 0.0, turnSpeed);
		
	}
}