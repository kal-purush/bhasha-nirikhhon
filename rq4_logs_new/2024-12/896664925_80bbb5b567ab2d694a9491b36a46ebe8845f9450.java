package frc.robot.subsystems.ballMechanism;

import edu.wpi.first.wpilibj2.command.Command;
import edu.wpi.first.wpilibj2.command.Commands;
import edu.wpi.first.wpilibj2.command.SubsystemBase;

public class ballMechanism extends SubsystemBase {
    ballMechanismIO io;
    ballMechanismInputsAutoLogged intakeAutoLogged = new ballMechanismInputsAutoLogged();

    public ballMechanism(ballMechanismIO io) {
        this.io = io;

    }

    @Override
    public void periodic() {

    }

    public void setBallMotor(double power) {
        io.setBallMotor(power);
        io.updateInputs(intakeAutoLogged);
    }

    public boolean getIntakeBeamBreakStatus() {
        return intakeAutoLogged.intakeBeamBrake;
    }

    public boolean getOuttakeBeamBrakeStatus() {
        return intakeAutoLogged.outtakeBeamBrake;
    }

    public Command intakeCommand() {
        return Commands.startEnd(() -> {
            setBallMotor(1);
        }, () -> {
            setBallMotor(0);
        }, this).until(() -> getIntakeBeamBreakStatus()).unless(() -> getIntakeBeamBreakStatus());

    }

    public Command outtakeCommand() {
        return Commands.startEnd(() -> {
            setBallMotor(1);
        }, () -> {
            setBallMotor(0);
        }, this).until(() -> getOuttakeBeamBrakeStatus()).withTimeout(1);
    }
};
