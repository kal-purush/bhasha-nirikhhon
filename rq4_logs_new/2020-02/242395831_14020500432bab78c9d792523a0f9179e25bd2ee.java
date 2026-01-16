/*----------------------------------------------------------------------------*/
/* Copyright (c) 2018 FIRST. All Rights Reserved.                             */
/* Open Source Software - may be modified and shared by FRC teams. The code   */
/* must be accompanied by the FIRST BSD license file in the root directory of */
/* the project.                                                               */
/*----------------------------------------------------------------------------*/

package frc.robot.subsystems;

import edu.wpi.first.wpilibj.command.Subsystem;
import edu.wpi.first.wpilibj.Encoder;

import frc.robot.RobotMap;
import frc.robot.Robot;

public class Arm extends Subsystem {
  //Create encoder object
  public Encoder armEncoder = new Encoder(RobotMap.armEncoderPorts[0], RobotMap.armEncoderPorts[1], false, Encoder.EncodingType.k4X);

  @Override
  public void initDefaultCommand() {
    // Set the default command for a subsystem here.
    // setDefaultCommand(new MySpecialCommand());
  }
}