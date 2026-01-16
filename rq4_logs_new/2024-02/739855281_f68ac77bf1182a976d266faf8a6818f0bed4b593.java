package frc.robot.commands;

import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;

import edu.wpi.first.math.geometry.Pose2d;
import edu.wpi.first.wpilibj2.command.Command;
import frc.robot.subsystems.LimlihSubsystem;
import frc.robot.subsystems.PoseEstimationSubsystem;
import frc.robot.subsystems.swerve.Drivetrain;

public class LimDriveSetCommand extends Command {

    private LimlihSubsystem limlihSubsystem;
    private Drivetrain drivetrain;
    private PoseEstimationSubsystem poseEstimationSubsystem;
 
    public LimDriveSetCommand(LimlihSubsystem limlihSubsystem, Drivetrain drivetrain, PoseEstimationSubsystem poseEstimationSubsystem) {

        this.limlihSubsystem = limlihSubsystem;
        this.drivetrain = drivetrain;
        this.poseEstimationSubsystem = poseEstimationSubsystem;



    }

    @Override
    public void initialize() {
        Pose2d carl = limlihSubsystem.getRobotPose();

        if (limlihSubsystem.seeingAnything() && !limlihSubsystem.getRobotPose().equals(new Pose2d()))
            drivetrain.resetOdometry(carl);

    }

    @Override
    public boolean isFinished() {
        return true;
    }    


    

}