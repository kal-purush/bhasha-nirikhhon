package org.usfirst.frc.team346.robot;

import org.usfirst.frc.team346.subsystem.Shooter;
import org.usfirst.frc.team346.subsystem.Drive;

import edu.wpi.first.wpilibj.Compressor;
import edu.wpi.first.wpilibj.IterativeRobot;
import edu.wpi.first.wpilibj.Joystick;
import edu.wpi.first.wpilibj.buttons.Button;
import edu.wpi.first.wpilibj.buttons.JoystickButton;

public class Robot extends IterativeRobot {

	private Joystick m_leftJoystick;
	private Button m_leftTrigger;
	private Joystick m_rightJoystick;
	
	private Compressor m_compressor;
	
	//private Shooter m_shooterSubsystem;
	private Drive m_drive;


    public void robotInit() {
    	//this.m_shooterSubsystem = new Shooter(RobotMap.LEFT_SHOOTER_MOTOR_PORT);
    	this.m_drive = new Drive(RobotMap.LEFT_DRIVE_MASTER_PORT,
    							 RobotMap.LEFT_DRIVE_SLAVE_PORT,
    							 RobotMap.RIGHT_DRIVE_MASTER_PORT,
    							 RobotMap.RIGHT_DRIVE_SLAVE_PORT);
    	this.m_leftJoystick = new Joystick(RobotMap.LEFT_JOYSTICK_PORT);
    	this.m_leftTrigger = new JoystickButton(this.m_leftJoystick, 1);
    	this.m_rightJoystick = new Joystick(RobotMap.RIGHT_JOYSTICK_PORT);
    	
    	this.m_compressor = new Compressor();
    	this.m_compressor.start();
    }
   
	public void teleopPeriodic() {
    	//this.m_shooterSubsystem.setVoltage();
    	this.m_drive.setDrive(m_leftJoystick.getY(),
    						  m_rightJoystick.getY());
    	// Insert code to read when m_leftTrigger is pressed
	}
	
}