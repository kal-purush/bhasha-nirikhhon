package frc.robot.commands;

import edu.wpi.first.wpilibj.sysid.SysIdRoutineLog;
import edu.wpi.first.wpilibj2.command.Command;
import edu.wpi.first.wpilibj2.command.sysid.SysIdRoutine;
import frc.robot.subsystems.ShootSubsystem;


public class IDFlywheelCommand {
    ShootSubsystem shootSubsystem;
    SysIdRoutine sysIdRoutine;
    SysIdRoutineLog.State state;

    public IDFlywheelCommand(ShootSubsystem shootSubsystem, SysIdRoutineLog.State state) {
        this.shootSubsystem = shootSubsystem;
        this.state = state;
        sysIdRoutine = new SysIdRoutine(new SysIdRoutine.Config(), new SysIdRoutine.Mechanism(shootSubsystem::setVoltage, this::getData, shootSubsystem));
    }

    private void getData(SysIdRoutineLog sysIdRoutineLog) {
        sysIdRoutineLog.motor("Shoot");
        sysIdRoutineLog.recordState(state);
    }
}