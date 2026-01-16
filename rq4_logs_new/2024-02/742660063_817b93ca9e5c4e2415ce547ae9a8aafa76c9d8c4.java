package frc.robot.RotationLocker;

import edu.wpi.first.math.controller.PIDController;

public class SwerveAnglePID {
    public static PIDController rotationPID = createPIDController();
    private static PIDController createPIDController() {
        PIDController pid = new PIDController(0.0015, 0.0015, 0.00015);
        pid.setTolerance(1); // allowable angle error
        pid.enableContinuousInput(0, 360); // it is faster to go 1 degree from 359 to 0 instead of 359 degrees
        pid.setSetpoint(0); // 0 = apriltag angle/offset
        return pid;
    }

    public double getAnglePIDVal(double angleError) {
        double calculatedValue = rotationPID.calculate(angleError);
        return calculatedValue;
    }

    

}