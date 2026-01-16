// Copyright (c) FIRST and other WPILib contributors.
// Open Source Software; you can modify and/or share it under the terms of
// the WPILib BSD license file in the root directory of this project.

package frc.robot.commands;

import edu.wpi.first.wpilibj2.command.CommandBase;
import frc.robot.Constants;
import frc.robot.subsystems.DrivetrainSubsystem;

public class DriveForwardPerInch extends CommandBase {
  /** Creates a new DriveForwardPerInch. */
  DrivetrainSubsystem drivetrainSub;

  double inches;
  double finalPosition;
  double drivespeed;

  boolean driveForwardPerInchIsFinished;

  public DriveForwardPerInch(DrivetrainSubsystem drivetrainSub, double inches, double drivespeed) {
    // Use addRequirements() here to declare subsystem dependencies.

    this.drivetrainSub = drivetrainSub;
    this.inches = inches;

    addRequirements(drivetrainSub);
  }

  // Called when the command is initially scheduled.
  @Override
  public void initialize() {

    finalPosition = drivetrainSub.leftTopMotorPos + (inches * Constants.DrivetrainConstants.HIGH_GEAR_ENCODER_PER_INCH);
    driveForwardPerInchIsFinished = false;
  }

  // Called every time the scheduler runs while the command is scheduled.
  @Override
  public void execute() {

    if (finalPosition > drivetrainSub.leftTopMotorPos) {
      drivetrainSub.driveForward(drivespeed);
    } else {
      drivetrainSub.driveForward(0);
      driveForwardPerInchIsFinished = true;
    }
  }

  // Called once the command ends or is interrupted.
  @Override
  public void end(boolean interrupted) {
    drivetrainSub.driveForward(0);
  }

  // Returns true when the command should end.
  @Override
  public boolean isFinished() {
    if (driveForwardPerInchIsFinished == true) {
      return true;
    } else {
      return false;
    }
  }
}