package frc.robot.subsystems.hatch;

import edu.wpi.first.math.controller.PIDController;
import edu.wpi.first.math.geometry.Rotation2d;
import edu.wpi.first.wpilibj2.command.Command;
import edu.wpi.first.wpilibj2.command.Commands;
import edu.wpi.first.wpilibj2.command.SubsystemBase;
import edu.wpi.first.wpilibj2.command.button.CommandXboxController;

public class Hatch extends SubsystemBase {
    public HatchIO io;
    CommandXboxController controller;
    private Boolean pidEnabled = false;
    private static final Rotation2d upright = Rotation2d.fromDegrees(100);
    private static final Rotation2d neutral = Rotation2d.fromDegrees(10);

    public Hatch(HatchIO io, CommandXboxController controller) {
        this.io = io;
        this.controller = controller;

    }

    @Override
    public void periodic() {}



    PIDController hatchPidController = new PIDController(0, 0, 0);

    public Boolean atGoal() {
        return hatchPidController.atSetpoint();
    }

    public Command goToPosition(Rotation2d angle) {
        return Commands.runOnce(() -> {
            hatchPidController.reset();
            hatchPidController.setSetpoint(angle.getDegrees());
            pidEnabled = true;
        }).andThen(Commands.waitUntil(() -> atGoal()))
            .andThen(Commands.runOnce(() -> pidEnabled = false));
    }

    public Command hatchUpCommand() {
        return goToPosition(upright);
    }

    public Command hatchNeutralCommand() {
        return goToPosition(neutral);
    }


}