/*----------------------------------------------------------------------------*/
/* Copyright (c) 2016 FIRST Team 2035. All Rights Reserved.                   */
/* Open Source Software - may be modified and shared by FRC teams.            */
/*----------------------------------------------------------------------------*/
package org.usfirst.frc.team2035.robot.commands;

import edu.wpi.first.wpilibj.DoubleSolenoid;
import edu.wpi.first.wpilibj.command.Command;
import edu.wpi.first.wpilibj.smartdashboard.SmartDashboard;

import org.usfirst.frc.team2035.robot.Robot;
import org.usfirst.frc.team2035.robot.OI;
import org.usfirst.frc.team2035.robot.subsystems.SpikeODeath;

/**
 * The base for all commands. All commands should be a child of CommandBase.
 * CommandBase stores and creates each control system.
 * @author Team 2035's idiots
 */
public  class SpikeDown extends CommandBase {


    public static OI oi;
    private final SpikeODeath spike;
    // Create a single static instance of all of your subsystems

    
    public SpikeDown() {
        super("Spike Retract");
        spike = Robot.getSpike();
        requires(spike);
    }
    

    protected void initialize() {
        // This MUST be here. If the OI creates Commands (which it very likely
        // will), constructing it during the construction of CommandBase (from
        // which commands extend), subsystems are not guaranteed to be
        // yet. Thus, their requires() statements may grab null pointers. Bad
        // news. Don't move it.
        
    	oi = new OI();
        // Show what command your subsystem is running on the SmartDashboard
        //SmartDashboard.putData(spike);
    }

	
	protected void execute() {
		spike.setSpikeDown();
	}
	
	protected void end() {
		spike.setSpikeStop();
		spike.lockSpike();
	}
	
	protected void interrupted() {
		spike.setSpikeStop();
		spike.lockSpike();
	}
	
	protected boolean isFinished() {
		return false;
	}
	
}