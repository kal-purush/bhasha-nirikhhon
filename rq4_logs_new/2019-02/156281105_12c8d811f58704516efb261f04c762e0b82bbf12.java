package frc.team4909.robot.subsystems.climber.commands;

import edu.wpi.first.wpilibj.command.Command;
import edu.wpi.first.wpilibj.smartdashboard.SmartDashboard;
import frc.team4909.robot.Robot;
import frc.team4909.robot.RobotConstants;
import frc.team4909.robot.operator.controllers.BionicF310;

public class SetStiltPosition extends Command {
    private int holdingPosition;

    public SetStiltPosition(){
        requires(Robot.climberSubsystem);
        holdingPosition = Robot.climberSubsystem.getPosition();
    }

     @Override
    protected void execute() {
        double moveSpeed = -Robot.climberGamepad.getThresholdAxis(BionicF310.LY)
                * RobotConstants.climbSpeedMultiplier;
        if (moveSpeed == 0) { // If Y-stick value is not moving, HOLD position
            Robot.climberSubsystem.setStiltsPosition(holdingPosition);
            SmartDashboard.putNumber("Holding Pos", holdingPosition);
            SmartDashboard.putNumber("Move speed", moveSpeed);


        } else { // Set speed to Y-stick value and HOLD position
            Robot.climberSubsystem.setStiltsClimbSpeed(moveSpeed);
            holdingPosition = Robot.climberSubsystem.getPosition();
            SmartDashboard.putNumber("Move speed", moveSpeed);



        }
        System.out.println("Climber Position " + Robot.climberSubsystem.getPosition());
    }
    @Override
    protected boolean isFinished() {
        return false;
    }


}