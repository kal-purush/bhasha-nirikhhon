package frc.robot.subsystems;

import org.littletonrobotics.junction.Logger;

import edu.wpi.first.wpilibj.Timer;
import edu.wpi.first.wpilibj2.command.SubsystemBase;

public class LoggingSubsystem extends SubsystemBase {

    private LoggedSubsystem[] subsystems;
    public Timer timer;

    public LoggingSubsystem(LoggedSubsystem... subsystems) {
        this.subsystems = subsystems;
        timer = new Timer();
        timer.start();
    }

    @Override
    public void periodic() {

        if (timer.hasElapsed(.5)) {

            for (int i = 0; i < subsystems.length; i += 2) {
                String name = subsystems[i].getClass().getSimpleName();
                Logger.processInputs(name, subsystems[i].log());
            }
        } else if (timer.hasElapsed(1)) {

            for (int i = 1; i < subsystems.length; i += 2) {
                String name = subsystems[i].getClass().getSimpleName();
                Logger.processInputs(name, subsystems[i].log());
            }
            timer.restart();
        }
    }

}