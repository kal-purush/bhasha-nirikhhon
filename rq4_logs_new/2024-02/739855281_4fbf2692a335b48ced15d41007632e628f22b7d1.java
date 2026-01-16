package frc.robot.commands.drive;

import edu.wpi.first.math.controller.PIDController;
import edu.wpi.first.networktables.GenericEntry;
import edu.wpi.first.wpilibj.shuffleboard.Shuffleboard;
import edu.wpi.first.wpilibj2.command.Command;
import frc.robot.subsystems.LimlihSubsystem;
import frc.robot.subsystems.swerve.Drivetrain;

public class DriveToTargetCommand extends Command {

    private final Drivetrain drivetrain;
    private final LimlihSubsystem limlihSubsystem;

    private final PIDController rotationPID;
    private final PIDController forwardsbackwardsPidController;
    private final PIDController leftrightPidController;

    private final int targetId;
    private final double targetDistance;

    GenericEntry oaijifsd = Shuffleboard.getTab("o;adssfa").add("output", 0).getEntry();
    GenericEntry IsTargetVis = Shuffleboard.getTab("o;adssfa").add("IsVis", false).getEntry();


    public DriveToTargetCommand(Drivetrain drivetrain, LimlihSubsystem limlihSubsystem, int targetId,
            double targetDistance) {
        this.drivetrain = drivetrain;
        this.limlihSubsystem = limlihSubsystem;



        this.rotationPID = new PIDController(1.25/2, 0, 0);//(1.25, 0, 0)
        rotationPID.setTolerance(0.2);

        this.forwardsbackwardsPidController = new PIDController(0.7/2, 0, 0.);//(0.7, 0, 0)
        forwardsbackwardsPidController.setTolerance(0.5);

        this.leftrightPidController = new PIDController(0.58/2, 0, 0);//(0.58, 0, 0.0001)
        leftrightPidController.setTolerance(0.38);



        this.targetId = targetId;
        this.targetDistance = targetDistance;

        addRequirements(drivetrain, limlihSubsystem);
    }

    @Override
    public void initialize() {
        rotationPID.setSetpoint(0);
        forwardsbackwardsPidController.setSetpoint(targetDistance);
        leftrightPidController.setSetpoint(0);
    }

    @Override
    public void execute() {
        if (limlihSubsystem.getTargetVisible(targetId)) {
                System.out.println("DriveToTarget Execute Called");

            double rotOutput = rotationPID.calculate(-limlihSubsystem.getCalculatedPoseRot(targetId));
            double forwardsbackwardsOutput = forwardsbackwardsPidController.calculate(limlihSubsystem.getTargetSpacePose(targetId).getZ());
            double leftrightOutput = leftrightPidController.calculate(-limlihSubsystem.getTargetSpacePose(targetId).getX());
            oaijifsd.setDouble(rotOutput);
            drivetrain.drive(forwardsbackwardsOutput, leftrightOutput, rotOutput, true);
        } else {
            drivetrain.drive(0, 0, 0, false);
        }
        IsTargetVis.setBoolean(limlihSubsystem.getTargetVisible(targetId));
    }

    @Override
    public boolean isFinished() {
        System.out.println(" DriveToTarget IsFinished Called " + rotationPID.atSetpoint() + " fwbw " + forwardsbackwardsPidController.atSetpoint() + " LR " + leftrightPidController.atSetpoint());
        return
        rotationPID.atSetpoint() &&
        forwardsbackwardsPidController.atSetpoint() &&
        leftrightPidController.atSetpoint();

    }

    @Override
    public void end(boolean interrupted) {
       System.out.println("DriveToTarget end Called");
        drivetrain.stop();
    }

}