package frc.team4909.robot.subsystems.climber.commands;

import edu.wpi.first.wpilibj.Timer;
import edu.wpi.first.wpilibj.command.Command;
import frc.team4909.robot.subsystems.elevator.ElevatorSubsystem;
import frc.team4909.robot.Robot;
import frc.team4909.robot.RobotConstants;
import frc.team4909.robot.operator.controllers.FlightStick;;

public class ClimbOI extends Command {


    public ClimbOI() {
        requires(Robot.climberSubsystem);
    }

    @Override
    public void execute() {
        int povAngle = Robot.climbStick.getPOV();
        // Sets speed to manipulator gamepad right Y stick value
        double moveSpeed = Robot.climbStick.getRawAxis(1) * RobotConstants.climbSpeedMultiplier;

        switch(povAngle){
            case(0):
                new ExtendStiltOnly();
            case(180):
                new RetractStiltOnly();
        }

        if (moveSpeed == 0) { // If Y-stick value is not moving, HOLD position
            Robot.climberSubsystem.setSpeeds(0);

        } else { // Set speed to Y-stick value and HOLD position
            Robot.climberSubsystem.setSpeeds(moveSpeed);

        } 
    }

    @Override
    protected boolean isFinished() {
        return false;
    }
}