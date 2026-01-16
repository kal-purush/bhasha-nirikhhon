package frc.robot.commands.elevatorCommands;

import java.util.function.Supplier;

import edu.wpi.first.wpilibj2.command.Command;
import edu.wpi.first.wpilibj2.command.button.CommandXboxController;
import frc.robot.subsystems.ElevatorSubsystem;

public class ElevatorManualCommand extends Command {

    private ElevatorSubsystem elevatorSubsystem;
    private Supplier<Double> rightTriggerSupplier;
    private Supplier<Double> leftTriggerSupplier;

    public ElevatorManualCommand(ElevatorSubsystem elevatorSubsystem, Supplier <Double> leftTriggerSupplier, Supplier <Double> rightTriggerSupplier) {

    }

    @Override
    public void initialize() {

    }

    @Override
    public void execute() {
        if (rightTriggerSupplier.get() > 0.1) {
            elevatorSubsystem.elevatorMove(rightTriggerSupplier.get());

        } else if (leftTriggerSupplier.get() > 0.1) {
            elevatorSubsystem.elevatorMove(-leftTriggerSupplier.get());
        } else {

            elevatorSubsystem.stop();
        }
    }

    @Override
    public boolean isFinished() {

        return false;
    }

    @Override
    public void end(boolean interrupted) {

    }

}