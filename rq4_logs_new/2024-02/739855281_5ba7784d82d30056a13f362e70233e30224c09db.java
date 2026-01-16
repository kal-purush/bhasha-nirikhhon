package frc.robot.commands;

import edu.wpi.first.wpilibj2.command.Command;
import frc.robot.subsystems.ElevatorSubsystem;

public class ElevatorDownCommand extends Command {

      public ElevatorSubsystem elevatorSubsystem;

    public ElevatorDownCommand(ElevatorSubsystem elevatorSubsystem) {
        this.elevatorSubsystem = elevatorSubsystem;
        
        addRequirements(elevatorSubsystem);

    }

    @Override
    public void execute() {
      
        elevatorSubsystem.elevatorDown();
    }

    @Override
    public boolean isFinished() {
        return false;
    }
    
    
    

}

    