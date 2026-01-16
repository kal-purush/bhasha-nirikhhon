package frc.team4909.robot.subsystems.climber.commands;

import edu.wpi.first.wpilibj.command.Command;
import frc.team4909.robot.Robot;
import frc.team4909.robot.RobotConstants;

public class BothLiftDown extends Command{

    public BothLiftDown(){
        requires(Robot.climberSubsystem);
        requires(Robot.elevatorSubsystem);
    }

    public void execute() {
        Robot.climberSubsystem.setStiltsClimbSpeed(RobotConstants.liftClimbSpeed);
            int climberPos = Robot.climberSubsystem.getPosition();
            int elevPos = (int) -(climberPos * (1.1/1.3));
            Robot.elevatorSubsystem.setPosition(elevPos);

            Robot.elevatorSubsystem.holdingPosition = elevPos;
            Robot.climberSubsystem.holdingStiltsPosition = climberPos;     
    }

    public void end(){
        Robot.climberSubsystem.setStiltsClimbSpeed(0);
    }

    @Override
    protected boolean isFinished() {
        return false;
    }
    
}