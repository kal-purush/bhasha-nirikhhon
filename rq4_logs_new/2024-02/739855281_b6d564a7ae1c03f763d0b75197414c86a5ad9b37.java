package frc.robot.commands;

import edu.wpi.first.wpilibj2.command.Command;
import frc.robot.subsystems.IndexSubsystem;
import frc.robot.subsystems.IntakeSubsystem;

public class IndexHoldCommand extends Command {

    private IndexSubsystem indexSubsystem;
    
    
    
    public IndexHoldCommand(IndexSubsystem indexSubsystem) {
      
        this.indexSubsystem = indexSubsystem;
        addRequirements(indexSubsystem);


    }



    @Override
    public void end(boolean interrupted) {
        
        indexSubsystem.in();

     
    }

    @Override
    public boolean isFinished() {
       return false;
    }


    
    
}