package org.usfirst.frc.team1711.robot.commands.auton;

import org.usfirst.frc.team1711.robot.Robot;

import edu.wpi.first.wpilibj.command.CommandGroup;

/**
 * @author Lou
 */
public class LongSwitch extends CommandGroup {

    public LongSwitch() {

    	addParallel(new TimedLift(10, .5));
    	addParallel(new AutoDrive(10));
    	addSequential(new Turn(10));
    	addSequential(new AutoDrive(10));
    	addSequential(new Turn(10));
    	addSequential(new AutoDrive(10));
    	addSequential(new TimedExpel(10));
    }
}