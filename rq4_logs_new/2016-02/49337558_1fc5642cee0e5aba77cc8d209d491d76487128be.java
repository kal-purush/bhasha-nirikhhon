package org.usfirst.frc.team5483.robot.commands;

import org.usfirst.frc.team5483.robot.subsystems.BCRS;
import org.usfirst.frc.team5483.robot.subsystems.Chassis;

import edu.wpi.first.wpilibj.command.Command;

public class CommandBase extends Command {
	
	public static BCRS bcrs;
	public static Chassis chassis;
	
	public static void init() {
		bcrs = new BCRS();
		chassis = new Chassis();
    }
	
	protected void initialize() {
	}
	
	protected void execute() {
	}
	
	protected void interrupted() {
	}
	
	protected void end() {
	}
	
	protected boolean isFinished() {
		return false;
	}

}