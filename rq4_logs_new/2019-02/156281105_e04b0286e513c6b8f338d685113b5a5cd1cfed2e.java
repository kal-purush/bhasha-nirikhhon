package frc.team4909.robot.subsystems.climber.commands;

import edu.wpi.first.wpilibj.command.Command;
import frc.team4909.robot.Robot;
import frc.team4909.robot.RobotConstants;

public class StiltsDownOnly extends Command{

    public StiltsDownOnly(){
        requires(Robot.climberSubsystem);
    }

    public void execute() {
        Robot.climberSubsystem.setStiltsClimbSpeed(RobotConstants.climberStiltSpeed);
    }

    public void end(){
        Robot.climberSubsystem.setStiltsClimbSpeed(0);
        Robot.climberSubsystem.holdingStiltsPosition = Robot.climberSubsystem.getPosition();
    }

    @Override
    protected boolean isFinished() {
        return false;
    }
    
}