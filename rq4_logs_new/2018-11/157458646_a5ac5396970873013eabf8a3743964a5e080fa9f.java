package frc.robot;

import com.ctre.phoenix.motorcontrol.ControlMode;
import com.ctre.phoenix.motorcontrol.can.TalonSRX;
import edu.wpi.first.wpilibj.Encoder;


class SwerveSystem{
    TalonSRX driveMotor;
    TalonSRX rotateMotor;
    Encoder encoder;

    public SwerveSystem(int driveMotorCAN, int rotateMotorCAN, int firstEncoderDIO, boolean encoderDir){
        driveMotor = new TalonSRX(driveMotorCAN);
        rotateMotor = new TalonSRX(rotateMotorCAN);
        encoder = new Encoder(firstEncoderDIO, firstEncoderDIO + 1, encoderDir, Encoder.EncodingType.k4X);
        encoder.setDistancePerPulse(1/1.2);
    }

    public void move(float speed){

    }

    public void rotateTo(double inputAngle){
        while(encoder.getDistance()%360 < inputAngle){
            rotateMotor.set(ControlMode.PercentOutput, .6);
        }
        rotateMotor.set(ControlMode.PercentOutput, 0);
    }

    public double getPos(){
        return encoder.getDistance()%360;
    }
}