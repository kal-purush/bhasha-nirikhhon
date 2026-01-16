// Copyright (c) FIRST and other WPILib contributors.
// Open Source Software; you can modify and/or share it under the terms of
// the WPILib BSD license file in the root directory of this project.

package frc.robot.commands;

import edu.wpi.first.wpilibj2.command.CommandBase;
import frc.robot.Constants;
import frc.robot.subsystems.DrivetrainSubsystem;
import frc.robot.subsystems.NavxSubsystem;

public class ChargingStationBalanceCommand extends CommandBase {
  /** Creates a new ChargingStationBalanceCommand. */

  DrivetrainSubsystem drivetrainSub;
  NavxSubsystem navxSub;

  public ChargingStationBalanceCommand(DrivetrainSubsystem drivetrainSub, NavxSubsystem navxSub) {
    // Use addRequirements() here to declare subsystem dependencies.

    this.drivetrainSub = drivetrainSub;
    this.navxSub = navxSub;
    addRequirements(drivetrainSub);
  }

  // Called when the command is initially scheduled.
  @Override
  public void initialize() {
    System.out.println("Starting Balancing!");
  }

  // Called every time the scheduler runs while the command is scheduled.
  @Override
  public void execute() {
    drivetrainSub.driveForward(Constants.DrivetrainConstants.CHARGING_STATION_BALANCING_SPEED * navxSub.getPitch());
  }

  // Called once the command ends or is interrupted.
  @Override
  public void end(boolean interrupted) {
    System.out.println("Balancing Ended!");
  }

  // Returns true when the command should end.
  @Override
  public boolean isFinished() {
    if(navxSub.getPitch() == 0){
      return true;
    } else{
      return false;
    }
  }
}