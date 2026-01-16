package org.usfirst.frc.team3630.robot;

import edu.wpi.first.wpilibj.PIDController;
import edu.wpi.first.wpilibj.PIDOutput;
import edu.wpi.first.wpilibj.PIDSource;
import edu.wpi.first.wpilibj.PIDSourceType;
import edu.wpi.first.wpilibj.SpeedController;
import edu.wpi.first.wpilibj.drive.MecanumDrive;

public class AutoDriveTrain extends MecanumDrive {
	
	class PIDfeeder implements PIDSource {
		private PIDSourceType sourceType;
		private double sourceValue;
		
		public void setPIDSourceType(PIDSourceType pidSource) {
			sourceType = pidSource;
		}

		@Override
		public PIDSourceType getPIDSourceType() {
			return sourceType;
		}

		@Override
		public double pidGet() {
			return sourceValue;
		}
		
		public void pidSet(double value) {
			sourceValue = value;
		}
		
	}
	class PIDsetter implements PIDOutput {
		private double out;
		public void pidWrite(double output) {
			out=output;
		}
		public double pidGet(){
			return out;
		}
	}
	class PIDSystem {
		public PIDfeeder feeder;
		public PIDsetter setter;
		public PIDController pid;
		
		public PIDSystem (double kp, double ki, double kd){
			pid = new PIDController(kp, ki, kd, feeder, setter);
		}
	}
	
	PIDSystem x;
	PIDSystem y;
	PIDSystem theta;
	
	public AutoDriveTrain(SpeedController frontLeftMotor, SpeedController rearLeftMotor,
			SpeedController frontRightMotor, SpeedController rearRightMotor) {
		super(frontLeftMotor, rearLeftMotor, frontRightMotor, rearRightMotor);
		x = new PIDSystem(.05,0,0);
		y = new PIDSystem(.05,0,0);
		theta = new PIDSystem(.05,0,0);
	}

	
	public void autoDrive () {
		super.driveCartesian(x.setter.pidGet(), y.setter.pidGet(), theta.setter.pidGet());
	}
}