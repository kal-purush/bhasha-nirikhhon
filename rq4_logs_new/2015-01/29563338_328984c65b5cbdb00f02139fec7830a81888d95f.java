package org.usfirst.frc.team997.robot.subsystems;

import edu.wpi.first.wpilibj.DoubleSolenoid;
import edu.wpi.first.wpilibj.SpeedController;

public class ElevatorSpeedController implements SpeedController {

	private SpeedController motor;
	private DoubleSolenoid brake;
	
	public ElevatorSpeedController(SpeedController m, DoubleSolenoid b) {
		motor = m;
		brake = b;
	}
	
	@Override
	public void pidWrite(double arg0) {
		motor.pidWrite(arg0);
	}

	@Override
	public void disable() {
		motor.disable();
	}

	@Override
	public double get() {
		return motor.get();
	}

	@Override
	public void set(double speed) {
		if (Math.abs(speed)< .05) {
			motor.set(0);
			brake.set(DoubleSolenoid.Value.kForward);
		} else {
			brake.set(DoubleSolenoid.Value.kReverse);
			motor.set(speed);
		}
	}

	@Override
	public void set(double arg0, byte arg1) {
		// TODO Auto-generated method stub

	}

}