package com.team2994.frc;

import com.team2994.frc.test.AnalogMenu;
import com.team2994.frc.test.BaseMenu;
import com.team2994.frc.test.DigitalIOClockMenu;
import com.team2994.frc.test.DigitalIOEncoderMenu;
import com.team2994.frc.test.DigitalIOMenu;
import com.team2994.frc.test.DigitalIOStateMenu;
import com.team2994.frc.test.DigitalMenu;
import com.team2994.frc.test.PWMMenu;
import com.team2994.frc.test.RelayMenu;
import com.team2994.frc.test.SolenoidMenu;
import com.team2994.frc.test.TopMenu;

import edu.wpi.first.wpilibj.DoubleSolenoid;
import edu.wpi.first.wpilibj.DriverStation;
import edu.wpi.first.wpilibj.DriverStationLCD;
import edu.wpi.first.wpilibj.SimpleRobot;
import edu.wpi.first.wpilibj.Timer;

/*
 * Main Class for the Robot!!!!!!
 */
public class AstechzRobot extends SimpleRobot {
	private DriverStation driverStation;
	
	public AstechzRobot(DriverStation driverStation) {
		this.driverStation = driverStation;
	}
	
	// m_ds already points to the DriverStation from the RobotBase class;
	
	boolean loaded;
	boolean loading;
	boolean armDown;

	public void robotInit() {
		// Print an I'M ALIVE message before anything else. NOTHING ABOVE THIS LINE.
		Subsystems.lcd.clear();
		Subsystems.lcd.println(DriverStationLCD.Line.kUser1, 0, Constants.ROBOT_TYPES[Subsystems.ROBOT_TYPE]);
		Subsystems.lcd.updateLCD();

		// initialize the subsystems
		Subsystems.initRobot();

		// Set the expiration to 1.5 times the loop speed.
		Subsystems.robotDrive.setExpiration(Constants.LOOP_PERIOD * 1.5);
		Subsystems.leftDriveEncoder.setReverseDirection(true);
	}
	
	// CheckLoad
	//	* Checks if the winch switch has been pressed.
	//		----> If yes, makes sure the state of the motors and variables is correct.
	//	* Should be called every tick of an operator control loop to ensure that
	//	  the motor is shut off as soon as possible.
	private boolean checkLoad() {
		if (!driverStation.isEnabled()) {
			Subsystems.rightWinch.set(0.0);
			Subsystems.leftWinch.set(0.0);
			loaded = false;
			loading = false;
			return false;
		}
		// Switch is normally closed
		if (loading && Subsystems.winchSwitch.get()) {
			Subsystems.rightWinch.set(0.0);
			Subsystems.leftWinch.set(0.0);
			// Stop and reset the timer
			Subsystems.loadingTimer.stop();
			Subsystems.loadingTimer.reset();
			loaded = true;
			loading = false;
			return false;
		}
		// Did the timer expire? Yikes shut down the winch
		if (Subsystems.loadingTimer.get() > 10) {
			Subsystems.rightWinch.set(0.0);
			Subsystems.leftWinch.set(0.0);
			// Stop and reset the timer
			Subsystems.loadingTimer.stop();
			Subsystems.loadingTimer.reset();
			loaded = false;
			loading = false;
			return false;
		}

		return true;
	}
	
	// InitiateLoad
	//	* Begins a load of the catapult by running the winch motor.
	private void initiateLoad() {
		if (!loaded) {
			Subsystems.rightWinch.set(Constants.WINCH_FWD);
			Subsystems.leftWinch.set(-Constants.WINCH_FWD);
			// Start a timer
			Subsystems.loadingTimer.start();
			loading = true;
		}
	}

	// LaunchCatapult
	//	* If in the correct state to launch (loaded), launches the catapult.
	private void launchCatapult() {
		if (loaded) {
			Subsystems.rightWinch.set(Constants.WINCH_FWD);
			Subsystems.leftWinch.set(-Constants.WINCH_FWD);
			for (int i = 0; i < 37; i++) {
				if (driverStation.isOperatorControl()) {
					Subsystems.robotDrive.arcadeDrive(Subsystems.rightStick.getY(),-Subsystems.rightStick.getX(), true);
				}
				Timer.delay(0.01);
			}
			Subsystems.rightWinch.set(0.0);
			Subsystems.leftWinch.set(0.0);
			loaded = false;
		}
	}

	private void drive(double speed, int dist) {
		Subsystems.leftDriveEncoder.reset();
		Subsystems.leftDriveEncoder.start();
		
		int reading = 0;
		dist = Math.abs(dist);
		
		// The encoder.Reset() method seems not to set Get() values back to zero,
		// so we use a variable to capture the initial value.
		Subsystems.lcd.println(DriverStationLCD.Line.kUser2, 0, "initial=" + Subsystems.leftDriveEncoder.get());
		Subsystems.lcd.updateLCD();

		// Start moving the robot
		Subsystems.robotDrive.drive(speed, 0.0);
		
		while ((driverStation.isAutonomous()) && (reading <= dist)) {
			reading = Math.abs(Subsystems.leftDriveEncoder.get());				
			Subsystems.lcd.println(DriverStationLCD.Line.kUser3, 0, "reading=" + reading);
			Subsystems.lcd.updateLCD();
		}

		Subsystems.robotDrive.drive(0.0, 0.0);
		
		Subsystems.leftDriveEncoder.stop();
	}
	
	// Test Autonomous
	private void TestAutonomous() {
		Subsystems.robotDrive.setSafetyEnabled(false);

		// STEP 1: Set all of the states.
		// SAFETY AND SANITY - SET ALL TO ZERO
		loaded = Subsystems.winchSwitch.get();
		loading = false;
		Subsystems.intake.set(0.0);
		Subsystems.rightWinch.set(0.0);
		Subsystems.leftWinch.set(0.0);

		// STEP 2: Move forward to optimum shooting position
		drive(-Constants.AUTO_DRIVE_SPEED, Constants.SHOT_POSN_DIST);

		// STEP 3: Drop the arm for a clean shot
		Subsystems.arm.set(DoubleSolenoid.Value.kForward);
		Timer.delay(1.0); // Ken

		// STEP 4: Launch the catapult
		launchCatapult();

		Timer.delay(1.0); // Ken

		if (driverStation.getDigitalIn(0)) {
			// STEP 5: Start the intake motor and backup to our origin position to pick up another ball
			initiateLoad();
			Subsystems.intake.set(-Constants.INTAKE_COLLECT);
			while (checkLoad()) {
				drive(Constants.AUTO_DRIVE_SPEED, Constants.SHOT_POSN_DIST);
			}
			Timer.delay(1.0);

			// STEP 6: Shut off the intake, bring up the arm and move to shooting position
			Subsystems.intake.set(0.0);
			Subsystems.arm.set(DoubleSolenoid.Value.kReverse);
			Timer.delay(1.0);
			drive(-Constants.AUTO_DRIVE_SPEED, Constants.SHOT_POSN_DIST);

			// Step 7: drop the arm for a clean shot and shoot
			Subsystems.arm.set(DoubleSolenoid.Value.kForward);
			
			// UNTESTED KICKED OFF FIELD
			Timer.delay(1.0); // Ken
			launchCatapult();
		}

		// Get us fully into the zone for 5 points
		drive(-Constants.AUTO_DRIVE_SPEED, Constants.INTO_ZONE_DIST - Constants.SHOT_POSN_DIST);

		// SAFETY AND SANITY - SET ALL TO ZERO
		Subsystems.intake.set(0.0);
		Subsystems.rightWinch.set(0.0);
		Subsystems.leftWinch.set(0.0);
	}
	
	// Real Autonomous
	//	* Code to be run autonomously for the first ten (10) seconds of the match.
	//	* Launch catapult
	//	* Drive robot forward ENCODER_DIST ticks.
	public void autonomous() {
		Subsystems.robotDrive.setSafetyEnabled(false);
		
		// STEP 1: Set all of the states.
		// SAFETY AND SANITY - SET ALL TO ZERO
		loaded = Subsystems.winchSwitch.get();
		loading = false;
		Subsystems.intake.set(0.0);
		Subsystems.rightWinch.set(0.0);
		Subsystems.leftWinch.set(0.0);
		
		// STEP 2: Move forward to optimum shooting position
		drive(-Constants.AUTO_DRIVE_SPEED, Constants.SHOT_POSN_DIST);
		
		// STEP 3: Drop the arm for a clean shot
		Subsystems.arm.set(DoubleSolenoid.Value.kForward);
		Timer.delay(1.0); // Ken
		
		// STEP 4: Launch the catapult
		launchCatapult();
		Timer.delay(1.0); // Ken
		
		// Get us fully into the zone for 5 points
		drive(-Constants.AUTO_DRIVE_SPEED, Constants.INTO_ZONE_DIST - Constants.SHOT_POSN_DIST);

		// SAFETY AND SANITY - SET ALL TO ZERO
		Subsystems.intake.set(0.0);
		Subsystems.rightWinch.set(0.0);
		Subsystems.leftWinch.set(0.0);
	}
	
	// HandleDriverInputs
	//	* Drive motors according to joystick values
	//	* Shift (Button 7 on left joystick)
	//		----> ASSUMES kForward = high gear
	private void handleDriverInputs() {
		if(Subsystems.leftStick.getEvent(Constants.BUTTON_SHIFT) == ButtonEntry.EVENT_OPENED) {
			// Shift into high gear.
			Subsystems.shifters.set(DoubleSolenoid.Value.kForward);
		} else if(Subsystems.leftStick.getEvent(Constants.BUTTON_SHIFT) == ButtonEntry.EVENT_CLOSED) {
			// Shift into low gear.
			Subsystems.shifters.set(DoubleSolenoid.Value.kReverse);
		}

		Subsystems.robotDrive.arcadeDrive(Subsystems.rightStick.getY(), -Subsystems.rightStick.getX(), true);
	}

	// HandleShooter
	//	* Manage winch motor state.
	//	* Toggles collection and eject mode (Gamepad button 4)
	//		----> ASSUMES positive values = collecting
	private void handleShooter() {
		if (Subsystems.gamepad.getEvent(Constants.BUTTON_LOAD) == ButtonEntry.EVENT_CLOSED) {
			initiateLoad();
		}
		if (loading) {
			checkLoad();
		}
		if (Subsystems.gamepad.getEvent(Constants.BUTTON_SHOOT) == ButtonEntry.EVENT_CLOSED) {
			launchCatapult();
		}
	}
	
	// HandleArm
	//	* Manage solenoids for arm up-down
	//		----> ASSUMES kForward on DoubleSolenoid is the down position.
	//	* Handle intake motors
	private void handleArm() {
		if (Subsystems.gamepad.getEvent(Constants.BUTTON_ARM) == ButtonEntry.EVENT_CLOSED && armDown) {
			Subsystems.arm.set(DoubleSolenoid.Value.kReverse);
			armDown = false;
		} else if (Subsystems.gamepad.getEvent(Constants.BUTTON_ARM) == ButtonEntry.EVENT_CLOSED) {
			Subsystems.arm.set(DoubleSolenoid.Value.kForward);
			armDown = true;
		}

		if (Subsystems.gamepad.getDPadEvent(Constants.BUTTON_INTAKE_COLLECT) == ButtonEntry.EVENT_CLOSED) {
			Subsystems.intake.set(Constants.INTAKE_COLLECT);
		} else if (Subsystems.gamepad.getDPadEvent(Constants.BUTTON_INTAKE_COLLECT) == ButtonEntry.EVENT_OPENED)	{
			Subsystems.intake.set(0.0);
		}

		if (Subsystems.gamepad.getDPadEvent(Constants.BUTTON_INTAKE_EJECT) == ButtonEntry.EVENT_CLOSED) {
			Subsystems.intake.set(Constants.INTAKE_EJECT);
		}
		if (Subsystems.gamepad.getDPadEvent(Constants.BUTTON_INTAKE_EJECT) == ButtonEntry.EVENT_OPENED) {
			Subsystems.intake.set(0.0);
		}
	}
	
	// HandleEject
	//	* Handle eject piston.
	private void handleEject() {
		if (Subsystems.gamepad.getEvent(Constants.BUTTON_PASS) == ButtonEntry.EVENT_CLOSED) {
			Subsystems.ejectTimer.start();
			Subsystems.eject.set(DoubleSolenoid.Value.kForward);
		}
		if (Subsystems.ejectTimer.get() > Constants.EJECT_WAIT) {
			Subsystems.ejectTimer.stop();
			Subsystems.ejectTimer.reset();
			Subsystems.eject.set(DoubleSolenoid.Value.kReverse);
		}
	}

	// RegisterButtons
	//	* Register all the buttons required
	private void registerButtons() {
		Subsystems.leftStick.enableButton(Constants.BUTTON_SHIFT);
		Subsystems.gamepad.enableButton(Constants.BUTTON_LOAD);
		Subsystems.gamepad.enableButton(Constants.BUTTON_SHOOT);
		Subsystems.gamepad.enableButton(Constants.BUTTON_ARM);
		Subsystems.gamepad.enableButton(Constants.BUTTON_PASS);
	}
	
	// Code to be run during the remaining 2:20 of the match (after Autonomous())
	//
	// OperatorControl
	//	* Calls all the above methods
	public void operatorControl() {
		// SAFETY AND SANITY - SET ALL TO ZERO
		Subsystems.intake.set(0.0);
		Subsystems.rightWinch.set(0.0);
		Subsystems.leftWinch.set(0.0);

		Subsystems.arm.set(DoubleSolenoid.Value.kReverse);

		/* TODO: Investigate. At least year's (GTR East) competition, we reached the conclusion that disabling this was 
		 * the only way we could get out robot code to work (reliably). Should this be set to false?
		 */ 
		Subsystems.robotDrive.setSafetyEnabled(false);

		Timer clock = null;
		int sanity = 0;
		int bigSanity = 0;

		loading = false;
		loaded = Subsystems.winchSwitch.get();

		registerButtons();
		Subsystems.gamepad.update();
		Subsystems.leftStick.update();

		Subsystems.compressor.start();

		while (driverStation.isOperatorControl() && driverStation.isEnabled()) {
			clock.start();

			handleDriverInputs();
			handleShooter();
			handleArm();

			while (clock.get() <= Constants.LOOP_PERIOD) { // add an IsEnabled???
				clock.reset();
			}
			sanity++;
			if (sanity >= 100) {
				bigSanity++;
				sanity = 0;
				Subsystems.lcd.println(DriverStationLCD.Line.kUser3, 0, ""+ bigSanity);
			}
			Subsystems.gamepad.update();
			Subsystems.leftStick.update();
			Subsystems.lcd.updateLCD();
		}

		// SAFETY AND SANITY - SET ALL TO ZERO
		Subsystems.intake.set(0.0);
		Subsystems.rightWinch.set(0.0);
		Subsystems.leftWinch.set(0.0);
	}
	
	public void test() {
		if (Subsystems.ROBOT_TYPE == Constants.PLY_BOY) {
			testPlyBoy();
		} else {
			testRobot();
		}
	}
	
	public void testRobot() {
		Subsystems.shifters.set(DoubleSolenoid.Value.kForward);

		Subsystems.leftDriveEncoder.start();
		Subsystems.leftDriveEncoder.reset();

		int start = Subsystems.leftDriveEncoder.get();

		while (driverStation.isTest()) {
			if (Subsystems.rightStick.getRawButton(7)) {
				Subsystems.robotDrive.arcadeDrive(Subsystems.rightStick.getY(), -Subsystems.rightStick.getX(), true);
			} else {
				Subsystems.robotDrive.arcadeDrive(Subsystems.rightStick.getY()/2, -Subsystems.rightStick.getX()/2, true);
			}

			if (Subsystems.gamepad.getEvent(4) == ButtonEntry.EVENT_CLOSED) {
				start = Subsystems.leftDriveEncoder.get();
			}

			Subsystems.lcd.println(DriverStationLCD.Line.kUser3, 0, "lde: " + (Subsystems.leftDriveEncoder.get() - start));
			Subsystems.lcd.updateLCD();

			Subsystems.gamepad.update();
		}
	}
	
	
	/**
	* Run the test program
	*/
	// The test program is organized as a set of hierarchical menus that are
	// displayed on the LCD on the driver station. Each menu is either a set
	// of submenus or is a menu controlling the use of a port (or ports) on 
	// one of the IO modules plugged into the cRIO. 
	// See base.h for a description of the test menu hierarchy

	// Simplified User's guide:
	// dpad up   : move the cursor up one menu item
	// dpad down : move the cursor down one menu item
	// dpad left : decrement the value in the selected menu item or
	//             return to the previous menu if the menu item is "Back"
	// dpad right: increment the value in the selected menu item or
	//             enter a submenu (as appropriate)
	// gamepad right joystick: control the configured and enabled PWM ports
	//               (Top->Digital->PWM)

	void testPlyBoy()
	{
		int currentMenu = BaseMenu.TOP;
		int newMenu = BaseMenu.TOP;

		BaseMenu menus[] = new BaseMenu[BaseMenu.NUM_MENU_TYPE];

		menus[BaseMenu.TOP] = new TopMenu();
		menus[BaseMenu.ANALOG] = new AnalogMenu();
		menus[BaseMenu.DIGITAL_TOP] = new DigitalMenu();
		menus[BaseMenu.SOLENOID] = new SolenoidMenu();
		menus[BaseMenu.DIGITAL_PWM] = new PWMMenu();
		menus[BaseMenu.DIGITAL_IO] = new DigitalIOMenu();
		menus[BaseMenu.DIGITAL_RELAY] = new RelayMenu();
		menus[BaseMenu.DIGITAL_IO_STATE] = new DigitalIOStateMenu();
		menus[BaseMenu.DIGITAL_IO_CLOCK] = new DigitalIOClockMenu();
		menus[BaseMenu.DIGITAL_IO_ENCODER] = new DigitalIOEncoderMenu();

		// Write out the TOP menu for the first time
		menus[currentMenu].updateDisplay();

		// Initialize the button states on the gamepad
		Subsystems.gamepad.update();

		// Loop counter to ensure that the program us running (debug helper
		// that can be removed when things get more stable)
		int sanity = 0;

		while (driverStation.isTest()) {
			// The dpad "up" button is used to move the menu pointer up one line
			// on the LCD display
			if (Subsystems.gamepad.getDPadEvent(Gamepad.DPAD_DIRECTION_UP) == ButtonEntry.EVENT_CLOSED) {
				menus[currentMenu].handleIndexUp();
			}

			// The dpad "down" button is used to move the menu pointer down one line
			// on the LCD display
			if (Subsystems.gamepad.getDPadEvent(Gamepad.DPAD_DIRECTION_DOWN) == ButtonEntry.EVENT_CLOSED) {
				menus[currentMenu].handleIndexDown();
			}

			// The dpad left button is used to exit a submenu when the menu pointer
			// points to the "back" menu item and to decrease a value (where 
			// appropriate) on any other menu item.
			if (Subsystems.gamepad.getDPadEvent(Gamepad.DPAD_DIRECTION_LEFT) == ButtonEntry.EVENT_CLOSED) {
				newMenu = menus[currentMenu].handleSelectLeft();
			}

			// Theoretically, both the select buttons could be pressed in the 
			// same 10 msec window. However, if using the dpad on the game 
			// game controller this is physically impossible so we don't
			// need to worry about a previous value of newMenu being 
			// overwritten in the next bit of code.

			// The dpad right button is used to enter a submenu when the menu pointer
			// points to a submenu item and to increase a value (where  appropriate) 
			// on any other menu item.
			if (Subsystems.gamepad.getDPadEvent(Gamepad.DPAD_DIRECTION_RIGHT) == ButtonEntry.EVENT_CLOSED) {
				newMenu = menus[currentMenu].handleSelectRight();

				// Handle change from one menu to a sub menu
				if (newMenu != currentMenu) {
					// When we enter a menu we need to set the record the
					// menu to return to. We do *not* want to do this when
					// returning from a menu to its calling menu.
					menus[newMenu].setCallingMenu(currentMenu);
				}
			}

			// Handle change from one menu to another
			if (newMenu != currentMenu) {
				menus[newMenu].updateDisplay();
				currentMenu = newMenu;
			}

			// Set the motor speed(s) (if any have been enabled via the Digital PWM menu)
			menus[BaseMenu.DIGITAL_PWM].setSpeed(-1.0 * Subsystems.gamepad.getRightY());

			// Update gamepad button states
			Subsystems.gamepad.update();

			// Update the display (we do this on every loop pass because some menus
			// (analog, for example) need to have values updated even when there are
			// no dpad events to handle)
			menus[currentMenu].updateDisplay();

			// Dump the sanity time value to the LCD
			Subsystems.lcd.println(DriverStationLCD.Line.kUser6, 0, "Sanity: " + sanity);
			Subsystems.lcd.updateLCD();

			sanity++;

			// Run the loop every 50 msec (20 times per second)
			Timer.delay(0.050);
		}
		
		for (int i = 0; i < BaseMenu.NUM_MENU_TYPE; i++) {
			BaseMenu menu = menus[i];
			menu.cleanup();
		}
	}
}