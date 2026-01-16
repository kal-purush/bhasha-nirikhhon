package frc.robot.utils;

public class PIDController {
    private KGains gains;
    private boolean relationshipIsPositive;
    private double accumulatedError;
    private double lastMeasurement;
    private boolean lastMeasurementIsMeaningful;
    private double setpoint;
    public PIDController(KGains gains, boolean relationshipIsPositive) {
        this.gains = gains;
        this.relationshipIsPositive = relationshipIsPositive;
    }
    public void reset() {
        lastMeasurementIsMeaningful = false;
        accumulatedError = 0;
    }
    public void setSetpoint(double setpoint) {
        this.setpoint = setpoint;
    }
    private double getPTerm(double error) {
        if(!relationshipIsPositive) {
            return -gains.kP * error;
        }
        return gains.kP * error;
    }
    private double getITerm(double error) {
        if(error * accumulatedError < 0) {
            //If the error is opposite of the error that has been accumulated
            //(i.e., we have crossed the setpoint and gone to the other side)
            //then error is zeroed.
            accumulatedError = 0;
        }
        accumulatedError += error;
        if(!relationshipIsPositive) {
            return -gains.kI * error;
        }
        return accumulatedError * gains.kI;
    }
    private double getDTerm(double measurement) {
        if(!lastMeasurementIsMeaningful) {
            lastMeasurement = measurement;
            lastMeasurementIsMeaningful = true;
            return 0;
        }
        double ret = -(measurement - lastMeasurement) * gains.kD;
        if(!relationshipIsPositive) {
            ret = -ret;
        }
        lastMeasurement = measurement;
        return ret;
    }
    public double calculate(double setpoint, double measurement) {
        double error = measurement - setpoint;
        return getPTerm(error) + getITerm(error) + getDTerm(measurement);
    }
    public double calculate(double measurement) {
        double error = measurement - setpoint;
        return getPTerm(error) + getITerm(error) + getDTerm(measurement);
    }
}