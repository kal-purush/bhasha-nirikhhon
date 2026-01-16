// Copyright (c) FIRST and other WPILib contributors.
// Open Source Software; you can modify and/or share it under the terms of
// the WPILib BSD license file in the root directory of this project.

package frc.robot.commands;

import edu.wpi.first.wpilibj2.command.CommandBase;
import frc.robot.subsystems.ArmSubsystem;

public class SetArmAngleCommand extends CommandBase {
  /** Creates a new SetArmAngleCommand. */

  ArmSubsystem armSub;
  double armAngle;

  public SetArmAngleCommand(ArmSubsystem armSub, double armAngle) {
    // Use addRequirements() here to declare subsystem dependencies.

    this.armSub = armSub;
    this.armAngle = armAngle;
    addRequirements(armSub);
  }

  // Called when the command is initially scheduled.
  @Override
  public void initialize() {}

  // Called every time the scheduler runs while the command is scheduled.
  @Override
  public void execute() {
    armSub.setArmAngle(armAngle);
  }

  // Called once the command ends or is interrupted.
  @Override
  public void end(boolean interrupted) {}

  // Returns true when the command should end.
  @Override
  public boolean isFinished() {
    return false;
  }
}