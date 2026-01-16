package frc.team4909.robot.subsystems.intake;

import edu.wpi.first.wpilibj.command.Command;
import frc.team4909.robot.Robot;
import frc.team4909.robot.RobotConstants;

public class CargoIntakeHold extends Command{
    public void CargoIntakeHold(){
        requires(Robot.intakeSubsystem);
    }

    protected void initialize() {
        Robot.intakeSubsystem.setCargoIntakeSpeed(RobotConstants.cargoIntakeHoldSpeed);
    }

    @Override
    protected boolean isFinished() {
        return false;
    }
}