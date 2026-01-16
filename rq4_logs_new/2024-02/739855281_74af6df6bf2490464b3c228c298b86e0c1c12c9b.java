package frc.robot.commands;

import edu.wpi.first.wpilibj2.command.Command;
import frc.robot.subsystems.ElevatorSubsystem;
import frc.robot.utilities.ElevatorSetpoints;

public class ElevatorToAmpCommand extends Command {
    public ElevatorSubsystem elevatorSubsystem;

    public ElevatorToAmpCommand(ElevatorSubsystem elevatorSubsystem) {
        this.elevatorSubsystem = elevatorSubsystem;
        addRequirements(elevatorSubsystem);

    }

    @Override
    public void execute() {
        elevatorSubsystem.setElevatorPose(ElevatorSetpoints.AMPPOINT);;
    }

    @Override
    public boolean isFinished() {
        return elevatorSubsystem.atSetpoint();
    }
}