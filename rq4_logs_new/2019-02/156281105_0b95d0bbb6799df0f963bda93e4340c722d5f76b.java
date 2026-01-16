package frc.team4909.robot.subsystems.climber.commands;

import edu.wpi.first.wpilibj.command.Command;
import frc.team4909.robot.Robot;
import frc.team4909.robot.RobotConstants;
import frc.team4909.robot.operator.controllers.BionicF310;
import frc.team4909.robot.subsystems.elevator.ElevatorSubsystem;

/* TODO: Make sure this command is continuously being executed*/

public class ElevatorSynchronization extends Command {

    public ElevatorSynchronization() {
        requires(Robot.climberSubsystem);
        requires(Robot.elevatorSubsystem);
    }

    protected void execute() {
        if (Robot.elevatorSubsystem.getPosition() < 0) { // TODO: Make sure this number is the position that is returned
                                                         // when the elevator is at the bottom of the robot
            Robot.climberSubsystem.setStiltsClimbSpeed(
                    Robot.manipulatorGamepad.getThresholdAxis(BionicF310.LY) * RobotConstants.elevatorSpeedMultiplier);
        }
    }

    @Override
    protected void end() {
        Robot.climberSubsystem.setStiltsClimbSpeed(0);
        Robot.elevatorSubsystem.setSpeed(0);
    }

    @Override
    protected boolean isFinished() {
        return false;
    }
}