package org.usfirst.frc.team5053.robot.record_playback;


import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

import com.ctre.phoenix.motorcontrol.ControlMode;

import org.usfirst.frc.team5053.robot.RobotControllerMap;
import org.usfirst.frc.team5053.robot.Robot;


/*Code outline to implement playing back a macro recorded in BTMacroRecord
*Be sure to read out of the same file created in BTMacroRecord
*BEWARE OF: setting your motors in a different order than in BTMacroRecord and changing motor values before
*time is up. Both issues are dealt with and explained below. Also only read/write from/to the motors 
*you have fully coded for, otherwise your code will cut out for no reason. 
*In main, the try/catch structure catches any IOExceptions or FileNotFoundExceptions. Necessary to play back
*the recorded routine during autonomous
*Dennis Melamed and Melanie (sorta, she slept)
*March 22nd, 2015
*/


public class BTMacroPlay {
	Scanner scanner;
	long startTime; 
	
	int m_fileNumber;
	
	boolean debug = true;

	boolean onTime = true;
	double nextDouble;
	
	boolean done = false;
	

	public BTMacroPlay() throws FileNotFoundException
	{
		// Call the construtor using the max (i.e. latest) recorded file
		this(Robot.getMaxRecorderFileNumber());
	}
		
	public BTMacroPlay(int fileNumber) throws FileNotFoundException
	{
		m_fileNumber = fileNumber;
		//create a scanner to read the file created during BTMacroRecord
		//scanner is able to read out the doubles recorded into recordedAuto.csv (as of 2015)
		scanner = new Scanner(new File(getPlaybackFileName()));
		
		//let scanner know that the numbers are separated by a comma or a newline, as it is a .csv file
		scanner.useDelimiter(",|\\n");
		
		//lets set start time to the current time you begin autonomous
		startTime = System.currentTimeMillis();	
	}
	
	public String getPlaybackFileName() {
		return Robot.autoFile + m_fileNumber + ".csv";
	}
	
	public void play(RobotControllerMap theRobotControllerMap)
	{
		if(debug) {
			System.out.print("BTMacroRecord.play() entered");
		}
		
		//if recordedAuto.csv has a double to read next, then read it
		if ((scanner != null) && (scanner.hasNextDouble()))
		{
			double t_delta;
			
			//if we have waited the recorded amount of time assigned to each respective motor value,
			//then move on to the next double value
			//prevents the macro playback from getting ahead of itself and writing different
			//motor values too quickly
			if(onTime)
			{
				//take next value
				nextDouble = scanner.nextDouble();
			}
			
			//time recorded for values minus how far into replaying it we are--> if not zero, hold up
			t_delta = nextDouble - (System.currentTimeMillis()-startTime);
			
			//if we are on time, then set motor values
			if (t_delta <= 0)
			{
				//for 2015 robot. these are all the motors available to manipulate during autonomous.
				//it is extremely important to set the motors in the SAME ORDER as was recorded in BTMacroRecord
				//otherwise, motor values will be sent to the wrong motors and the robot will be unpredicatable
				
				//drive motors
				theRobotControllerMap.getLeftDriveGroup().set(scanner.nextDouble());
				theRobotControllerMap.getRightDriveGroup().set(scanner.nextDouble());
				
				//intake motors
				theRobotControllerMap.getLeftIntake().set(scanner.nextDouble());
				theRobotControllerMap.getRightIntake().set(scanner.nextDouble());
				
				//Elevator motor
				theRobotControllerMap.getElevator().set(ControlMode.PercentOutput,scanner.nextDouble());
				/*
				 * THE LAST ENTRY OF THINGS YOU RECORD NEEDS TO HAVE A DELIMITER CONCATENATED TO 
				 * THE STRING AT THE END. OTHERWISE GIVES NOSUCHELEMENTEXCEPTION
				 */
				if(debug) {
					System.out.print("in BTMacroRecord.play() "+(System.currentTimeMillis()-startTime)
						//drive motors		
						+"," + theRobotControllerMap.getLeftDriveGroup().get()
						+"," + theRobotControllerMap.getRightDriveGroup().get()
						//intake motors
						+"," + theRobotControllerMap.getLeftIntake().get()
						+"," + theRobotControllerMap.getRightIntake().get()
						//Elevator motor
						+"," + theRobotControllerMap.getElevator().getMotorOutputPercent()
						+ "\n"
					);
				}
				
				//this records a true/false value from a piston
//				theRobotControllerMap.getCatapult().set(scanner.nextBoolean() );

/*				storage.robot.getFrontLeftMotor().setX(scanner.nextDouble());
				storage.robot.getFrontRightMotor().setX(scanner.nextDouble());
				storage.robot.getBackRightMotor().setX(scanner.nextDouble());
				storage.robot.getBackLeftMotor().setX(scanner.nextDouble());
				
				storage.robot.getBarrelMotorLeft().setX(scanner.nextDouble());
				storage.robot.getBarrelMotorRight().setX(scanner.nextDouble());
				
				storage.robot.getLeftForkLeft().setX(scanner.nextDouble());
				storage.robot.getLeftForkRight().setX(scanner.nextDouble());
				storage.robot.getRightForkLeft().setX(scanner.nextDouble());
				storage.robot.getRightForkRight().setX(scanner.nextDouble());
				
				storage.robot.getToteClamp().set(storage.robot.getToteClamp().isExtended());
*/				
				//go to next double
				onTime = true;
			}
			//else don't change the values of the motors until we are "onTime"
			else
			{
				onTime = false;
			}
		}
		//end play, there are no more values to find
		else
		{
			if(debug) {
				System.out.print("in BTMacroRecord.play() no more values in the playback file "+getPlaybackFileName());
			}
			done = true;

			this.end(theRobotControllerMap);// This set all the motors to 0;
			if (scanner != null) 
			{
				scanner.close();
				scanner = null;
			}
		}
		
	}
	
	public boolean isDone() {return done;};
			
	//stop motors and end playing the recorded file
	public void end(RobotControllerMap theRobotControllerMap)
	{
		//drive motors
		theRobotControllerMap.getLeftDriveGroup().set(0);
		theRobotControllerMap.getRightDriveGroup().set(0);
		
		//intake motors
		theRobotControllerMap.getLeftIntake().set(0);
		theRobotControllerMap.getRightIntake().set(0);
		
		//Elevator motor
		theRobotControllerMap.getElevator().set(ControlMode.PercentOutput,0);

		/*
		 * THE LAST ENTRY OF THINGS YOU RECORD NEEDS TO HAVE A DELIMITER CONCATENATED TO 
		 * THE STRING AT THE END. OTHERWISE GIVES NOSUCHELEMENTEXCEPTION
		 */ 
		
		//this records a true/false value from a piston
//		theRobotControllerMap.getCatapult().set(false);

		/*		storage.robot.getFrontLeftMotor().setX(0);
		storage.robot.getBackLeftMotor().setX(0);
		storage.robot.getFrontRightMotor().setX(0);
		storage.robot.getBackRightMotor().setX(0);
		
		storage.robot.getBarrelMotorLeft().setX(0);
		storage.robot.getBarrelMotorRight().setX(0);
		
		storage.robot.getLeftForkLeft().setX(0);
		storage.robot.getLeftForkRight().setX(0);
		storage.robot.getRightForkLeft().setX(0);
		storage.robot.getRightForkRight().setX(0);
		//all this mess of a method does is keep the piston in the same state it ended in
		//if you want it to return to a specific point at the end of auto, change that here
		storage.robot.getToteClamp().set(storage.robot.getToteClamp().isExtended());
*/		
		if (scanner != null)
		{
			scanner.close();
		}
		
	}
	
}