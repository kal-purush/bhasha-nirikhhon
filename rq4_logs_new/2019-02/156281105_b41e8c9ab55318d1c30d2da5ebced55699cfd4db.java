package frc.team4909.robot.subsystems.elevator.commands;

import edu.wpi.first.wpilibj.command.Command;
import frc.team4909.robot.subsystems.elevator.ElevatorSubsystem;
import frc.team4909.robot.Robot;

public class ElevatorOperatorControl extends Command {
    private int setpoint;

    public ElevatorOperatorControl(int setpoint) {
        this.setpoint = setpoint;       
        requires(Robot.elevatorSubsystem);
    }

    @Override
    public void initialize() {
        Robot.elevatorSubsystem.holdingPosition = setpoint;
    }

    @Override
    protected boolean isFinished() {
        return false;
    }
}