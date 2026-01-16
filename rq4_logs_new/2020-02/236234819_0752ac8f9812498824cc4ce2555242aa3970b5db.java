/*----------------------------------------------------------------------------*/
/* Copyright (c) 2018 FIRST. All Rights Reserved.                             */
/* Open Source Software - may be modified and shared by FRC teams. The code   */
/* must be accompanied by the FIRST BSD license file in the root directory of */
/* the project.                                                               */
/*----------------------------------------------------------------------------*/

package frc.robot.subsystems;

import edu.wpi.first.wpilibj.DigitalInput;
import edu.wpi.first.wpilibj.PWMVictorSPX;
import edu.wpi.first.wpilibj.command.Subsystem;
import edu.wpi.first.wpilibj.smartdashboard.SmartDashboard;
import frc.robot.RobotMap;
import frc.robot.controls.MechanismsJoystick;

/**
 * Add your docs here.
 */
public class ConveyorBelt extends Subsystem {
 PWMVictorSPX lowerConveyor, upperConveyor;
 DigitalInput upperSensor, lowerSensorOne, lowerSensorTwo;
 Double speed, ballCount;
 Boolean lowerBallDetected, shooterIsSpeed;

 public ConveyorBelt(){
  lowerConveyor = RobotMap.lowerConveyorMotor;
  upperConveyor = RobotMap.upperConveyorMotor;
  lowerSensorOne = RobotMap.intakeSensorOne;
  lowerSensorTwo = RobotMap.intakeSensorTwo;

  upperSensor = RobotMap.shooterSensor;
  speed = 0.3;
  ballCount = 0.0;
  lowerBallDetected = false;
  shooterIsSpeed = false;

 }

  public void storeBalls(){
    if(lowerSensorOne.get() == true || lowerSensorTwo.get() == true && ballCount <= 5){
      lowerConveyor.set(speed);
      upperConveyor.set(speed);
      lowerBallDetected = true;
    }
    else{
      lowerConveyor.set(0);
      upperConveyor.set(0);
    }
    if(lowerBallDetected == true && lowerSensorOne.get() == false && lowerSensorTwo.get() == false){
      ballCount += 1;
      lowerBallDetected = false;
    }
  }

  public void conveyorOverride(){
    if(MechanismsJoystick.getToggleConveyorOverride() == true && upperSensor.get() == false){
      lowerConveyor.set(speed);
      upperConveyor.set(speed);
        }
    else{
      storeBalls();
    }
  }

  public void allowShooter(){
    if(MechanismsJoystick.getToggleShooter() && shooterIsSpeed){
      lowerConveyor.set(speed);
      upperConveyor.set(speed);
    }
    else{
      conveyorOverride();
    }
  }

  public void run(){
    conveyorOverride();
    SmartDashboard.putNumber("Number of Balls in Intake", ballCount);
  }


  @Override
  public void initDefaultCommand() {
    // Set the default command for a subsystem here.
    // setDefaultCommand(new MySpecialCommand());
  }
}