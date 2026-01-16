package org.usfirst.frc.team1711.robot.commands;

import org.usfirst.frc.team1711.robot.Robot;
import org.usfirst.frc.team1711.robot.commands.auton.AutoDrive;
import org.usfirst.frc.team1711.robot.commands.auton.Turn;

import edu.wpi.first.wpilibj.command.CommandGroup;

/**
 *
 */
public class LongSwitch extends CommandGroup {

    public LongSwitch() {
        // Add Commands here:
        // e.g. addSequential(new Command1());
        //      addSequential(new Command2());
        // these will run in order.

    	addParallel(new TimedLift(10, .5));
    	addParallel(new AutoDrive(10));
    	addSequential(new Turn(10));
    	addSequential(new AutoDrive(10));
    	addSequential(new Turn(10));
    	addSequential(new AutoDrive(10));
    	addSequential(new TimedExpel(10));
    	//drive backwards
    	
        // To run multiple commands at the same time,
        // use addParallel()
        // e.g. addParallel(new Command1());
        //      addSequential(new Command2());
        // Command1 and Command2 will run in parallel.

        // A command group will require all of the subsystems that each member
        // would require.
        // e.g. if Command1 requires chassis, and Command2 requires arm,
        // a CommandGroup containing them would require both the chassis and the
        // arm.
    }
}