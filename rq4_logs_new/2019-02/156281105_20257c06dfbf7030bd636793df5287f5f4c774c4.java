package frc.team4909.robot.subsystems.drivetrain;

import edu.wpi.first.wpilibj.DigitalInput;
import edu.wpi.first.wpilibj.command.Command;
import edu.wpi.first.wpilibj.smartdashboard.SmartDashboard;
import frc.team4909.robot.Robot;
import frc.team4909.robot.RobotConstants;

public class CameraLeftLineFollow extends Command {

    protected void initialize() {
    }

    protected void execute() {
        Robot.drivetrainSubsystem.arcadeDrive(-0.1, 0.1);
    }

    protected boolean isFinished() {
        return false;
    }
}