package frc.team4909.robot.subsystems.elevatorarm;

import frc.team4909.robot.Robot;
import frc.team4909.robot.RobotConstants;
import frc.team4909.robot.operator.controllers.BionicF310;
import frc.team4909.robot.subsystems.elevatorarm.*;
import edu.wpi.first.wpilibj.command.Command;


public class ElevatorArmOperatorControl extends Command {

    private int holdingPosition;
    public ElevatorArmOperatorControl(){
        requires(Robot.elevatorArmSubsystem);
    }

    @Override
    public void execute() {
        //Sets speed to manipulator gamepad right Y stick value
        double moveSpeed = -Robot.manipulatorGamepad.getThresholdAxis(BionicF310.LY) * RobotConstants.elevatorArmSpeedMultiplier;

        if(moveSpeed == 0 ) {  //If Y-stick value is not moving, HOLD position
            Robot.elevatorArmSubsystem.setPosition(holdingPosition);
        } 
        else { //Set speed to Y-stick value and HOLD position
            Robot.elevatorArmSubsystem.elevatorArmSetSpeed(moveSpeed);
            holdingPosition = Robot.elevatorArmSubsystem.getPosition();
        }

    }
    @Override
    protected boolean isFinished() {
        return false;
    }

}