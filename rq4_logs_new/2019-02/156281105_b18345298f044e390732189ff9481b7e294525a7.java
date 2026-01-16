package frc.team4909.robot.subsystems.elevatorarm;

import edu.wpi.first.wpilibj.command.InstantCommand;
import frc.team4909.robot.subsystems.elevatorarm.ElevatorArmSubsytem;

public class SetAngle extends InstantCommand {
    private final int setpoint;
    private final ElevatorArmSubsytem elevatorArm;

    public SetAngle(int setpoint, ElevatorArmSubsytem elevatorArm) {
        this.setpoint = setpoint;
        this.elevatorArm = elevatorArm;
    }

    @Override
    public void initialize() {
        elevatorArm.holdingPosition = setpoint;
    }
}