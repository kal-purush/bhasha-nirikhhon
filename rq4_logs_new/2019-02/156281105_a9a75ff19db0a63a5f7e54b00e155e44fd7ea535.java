package frc.team4909.robot.commands;

import edu.wpi.first.wpilibj.command.Command;
import frc.team4909.robot.Robot;
import frc.team4909.robot.subsystems.drivetrain.DriveTrainSubsystem;

public class InvertDriveDirection extends Command{
    private Drive drive;
    public InvertDriveDirection(){
        requires(Robot.drivetrainsub);
        drive = Robot.drivetrainsub.defaultCommand;
    }

    protected void initialize() {
        drive.speedMultiplier = -drive.speedMultiplier;
    }
    @Override
    protected boolean isFinished() {
        return false;
    }


}