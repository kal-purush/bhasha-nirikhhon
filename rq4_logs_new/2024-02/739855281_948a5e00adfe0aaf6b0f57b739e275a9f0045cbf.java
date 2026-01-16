package frc.robot.commands.armCommands;

import edu.wpi.first.wpilibj2.command.Command;
import frc.robot.subsystems.ArmAngleSubsystem;
import frc.robot.subsystems.ElevatorSubsystem;
import frc.robot.utilities.ArmAngle;

public class AutoZero extends Command {

    private ElevatorSubsystem elevatorSubsystem;
    private ArmAngleSubsystem armAngleSubsystem;

    public AutoZero(ElevatorSubsystem elevatorSubsystem, ArmAngleSubsystem armAngleSubsystem) {
        this.elevatorSubsystem = elevatorSubsystem;
        this.armAngleSubsystem = armAngleSubsystem;
    }

    
    
    @Override
    public void initialize() {
        
        
    }
    
     @Override
     public void execute() {
 
        armAngleSubsystem.incrementSetpoint(-0.3);
     }

    @Override
    public boolean isFinished() {

        return armAngleSubsystem.endSensor();
    }

     @Override
    public void end(boolean interrupted) {

        armAngleSubsystem.resetZero();
    }

    
}