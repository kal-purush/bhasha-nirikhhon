package frc.team4909.robot.subsystems.elevator;

import edu.wpi.first.wpilibj.command.InstantCommand;
import frc.team4909.robot.subsystems.elevator.ElevatorSubsystem;

public class SetElevatorPosition extends InstantCommand {
    private final int setpoint;
    private final ElevatorSubsystem elevator;

    public SetElevatorPosition(int setpoint, ElevatorSubsystem elevator) {
        this.setpoint = setpoint;
        this.elevator = elevator;
    }

    @Override
    public void initialize() {
        elevator.holdingPosition = setpoint;
    }
}