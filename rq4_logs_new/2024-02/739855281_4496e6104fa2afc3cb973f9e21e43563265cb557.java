package frc.robot.commands;

import edu.wpi.first.wpilibj.Timer;
import edu.wpi.first.wpilibj2.command.Command;
import frc.robot.subsystems.IndexSubsystem;
import frc.robot.subsystems.ShootSubsystem;

public class IndexFireCommand extends Command {

    private IndexSubsystem indexSubsystem;
    private ShootSubsystem shootSubsystem;
    private Timer timer;

    public IndexFireCommand(IndexSubsystem indexSubsystem, ShootSubsystem shootSubsystem) {

        this.indexSubsystem = indexSubsystem;
        this.timer = new Timer();
        addRequirements(indexSubsystem);

    }

    @Override
    public void initialize() {

    }

    @Override
    public void execute() {

        indexSubsystem.in();
        timer.start();

    }

    @Override
    public void end(boolean interrupted) {
        indexSubsystem.stop();
        shootSubsystem.stop();

    }

    @Override
    public boolean isFinished() {
        return timer.hasElapsed(0.33334);
    }

}