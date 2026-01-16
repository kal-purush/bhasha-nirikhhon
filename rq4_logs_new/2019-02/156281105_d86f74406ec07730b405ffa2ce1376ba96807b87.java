package frc.team4909.robot.subsystems.climber;

import com.ctre.phoenix.motorcontrol.can.WPI_TalonSRX;
import com.revrobotics.CANSparkMax;
import com.revrobotics.CANSparkMaxLowLevel.MotorType;
import edu.wpi.first.wpilibj.SpeedControllerGroup;
import edu.wpi.first.wpilibj.command.Subsystem;
import edu.wpi.first.wpilibj.drive.DifferentialDrive;
import frc.team4909.robot.RobotMap;
import frc.team4909.robot.RobotConstants;
import frc.team4909.robot.subsystems.drivetrain.commands.Drive;
import frc.team4909.robot.subsystems.climber.DriveStilts;
import frc.team4909.robot.subsystems.climber.RetractStilts;
import com.ctre.phoenix.motorcontrol.FeedbackDevice;

public class ClimberSubsystem extends Subsystem {
    WPI_TalonSRX climberSRX;

    public ClimberSubsystem() {
        climberSRX = new WPI_TalonSRX(RobotMap.);

        climberSRX.configSelectedFeedbackSensor(FeedbackDevice.CTRE_MagEncoder_Relative);

        /* PID yet to be calibrated */
        climberSRX.config_kP(1, 0.6, 0);
        climberSRX.config_kI(1, 0);
        climberSRX.config_kD(1, 0);
    }

    public void driveStilts() {
        climberSRX.set(RobotConstants.climberSpeed);
    }

    public void retractStilts() {
        climberSRX.set(-RobotConstants.climberSpeed);
    }
}