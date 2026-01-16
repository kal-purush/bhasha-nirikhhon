package frc.robot;

import com.ctre.phoenix.motorcontrol.ControlMode;
import com.ctre.phoenix.motorcontrol.can.TalonSRX;

import java.util.ArrayList;
import edu.wpi.first.wpilibj.DriverStation;
import edu.wpi.first.wpilibj.IterativeRobot;
import edu.wpi.first.wpilibj.Joystick;
import edu.wpi.first.wpilibj.Talon;
import edu.wpi.first.wpilibj.smartdashboard.SmartDashboard;
import edu.wpi.first.wpilibj.CameraServer;
import com.kauailabs.navx.frc.AHRS;
import com.kauailabs.navx.frc.AHRS.BoardAxis;



public class Robot extends IterativeRobot {
	
	final ControlMode PO = ControlMode.PercentOutput;
	
	final RobotUtil ru = new RobotUtil();
    
	Joystick pad, stick;
    TalonSRX leftWheelTalon, rightWheelTalon,liftTalon1; 
    Talon  liftTalon2, leftFeederTalon, rightFeederTalon;
    
    double rStart, autoStart, autoTime;
	int gameData;
	
	AHRS ahrs = new AHRS();
        

    

    
    ArrayList<Double> autoLengths;
    
    //[leftWheel, rightWheel, lift, intake]
    ArrayList<double[]> autoCommands;
    
    public void robotInit(){
    	
    	//CameraServer.getInstance().startAutomaticCapture();
    	//CameraServer.getInstance().startAutomaticCapture();

    	//upperLimitSwitch = new DigitalInput(9);
    	
    	pad = new Joystick(0);
    	stick = new Joystick(2);
    	leftWheelTalon = new TalonSRX(6);
    	rightWheelTalon = new TalonSRX(4);
    	liftTalon1 = new TalonSRX(3);
    	liftTalon2 = new Talon(2);
    	
    	leftFeederTalon = new Talon(0);
    	rightFeederTalon = new Talon(1);
    	 
        double power = 0;
    	leftWheelTalon.set(PO,power);
    	rightWheelTalon.set(PO,power);
    	liftTalon1.set(PO,power);
    	liftTalon2.set(power);
    	    	
    	leftFeederTalon.set(power);
    	rightFeederTalon.set(power);
    	
    	
    	
    	
    }
    
    public void teleopInit(){
		SmartDashboard.putNumber("R test", stick.getRawAxis(2));
		rStart = stick.getRawAxis(2);
    }
    		  
    public void teleopPeriodic() {
    	
    	//sd.putBoolean("Limit Switch", upperLimitSwitch.get());
    	
    	//y = forward and backward on Joystick
    	//r = "turning" left/right on Joystick
    	//t = throttle (.4 to 1)
    	//up = lift going up on left trigger
    	//down = lift going down on right trigger
    	//in = feed going in on left bumper
    	//out = feed going out on right bumper
    	//climbUp = climber going up on triangle
    	
    	SmartDashboard.putNumber("unproccessed t", stick.getRawAxis(3));
    	double t = ru.stickToT(stick.getRawAxis(3));
    	SmartDashboard.putNumber("proccessed t", t);
    	
    	double y = .9*t*-stick.getRawAxis(1);
    	SmartDashboard.putNumber("y", -stick.getRawAxis(1));
    	
    	double r = stick.getRawAxis(2) - rStart;
    	SmartDashboard.putNumber("r", stick.getRawAxis(2) - rStart);
    	
    	double up = (pad.getRawAxis(2));
    	SmartDashboard.putNumber("up", up);
    	
    	double down = (pad.getRawAxis(3));
    	SmartDashboard.putNumber("down", down);
    	
    	boolean in = pad.getRawButton(5);
        boolean out = pad.getRawButton(6);
        boolean rotate = pad.getRawButton(1);
        SmartDashboard.putBoolean("in", in);
        SmartDashboard.putBoolean("out", out);
        /*
         SmartDashboard.putBoolean(  "IMU_Connected",        ahrs.isConnected());
		 SmartDashboard.putBoolean(  "IMU_IsCalibrating",    ahrs.isCalibrating());
		*/
		 SmartDashboard.putNumber(   "IMU_Yaw",              ahrs.kBoardAxisX.getYaw());
		 /*
         SmartDashboard.putNumber(   "IMU_Pitch",            ahrs.getPitch());
		 SmartDashboard.putNumber(   "IMU_Roll",             ahrs.getRoll());
		 */
		

        leftWheelTalon.set(PO, ru.normalize(y+.65*r));
        rightWheelTalon.set(PO, ru.normalize(y-.65*r));
        
//        if(!upperLimitSwitch.get()) {
        	//liftTalon1.set(PO,.2*ru.normalize(up-.75*down) + .1);
        	//liftTalon2.set(.2*ru.normalize(up-.75*down) + .1);
//        }else {
        	liftTalon1.set(PO,.6*ru.normalize(up-.7*down)+.1);
        	liftTalon2.set(.6*ru.normalize(up-.7*down)+.1);
//        }
	    	
	    leftFeederTalon.set( ((in)?1:0) - ((out)?1:0));
	    rightFeederTalon.set( ((in)?1:0) - ((out)?1:0));
	    
	    
	    
	}
    
    public void testPeriodic() {
    	leftWheelTalon.set(PO, .4);
    	rightWheelTalon.set(PO, .4);
    }
    
    public void autonomousInit() {
    	
    	autoStart = System.currentTimeMillis();
    	autoTime = 0;
    	String raw = DriverStation.getInstance().getGameSpecificMessage();
    	gameData = (raw.charAt(0) == 'L')?-1:1;
    	
    	
    	SmartDashboard.putNumber("Switch Side", gameData); 
    	SmartDashboard.putNumber("Robot Placement", ru.pos);
    	
//    	autoLengths = new ArrayList<Double>();
//    	autoCommands = new ArrayList<double[]>();
//    	
//    	autoLengths.add(ru.initialForwardTime);
//    	autoCommands.add(ru.goForward);  
//    	
//    	autoLengths.add(ru.initialForwardTime+50);
//    	autoCommands.add(ru.goBackward);
//    	
//    	autoLengths.add(ru.initialForwardTime);
//    	autoCommands.add(ru.goForward);
//    	
////    	autoLengths.add(ru.toSwitchTime);
////    	autoCommands.add(ru.goForward);
////    	
////    	if()
//    	
//    	if(ru.pos == 0) {
//    		autoLengths.add(ru.turn90Time);
//    		autoCommands.add(ru.turn(gameData));
//    		
//    		autoLengths.add(ru.centerToSideTime);
//    		autoCommands.add(ru.goForward);
//    		
//    		autoLengths.add(ru.turn90Time);
//    		autoCommands.add(ru.turn(-gameData));
//    		
//    		ru.pos = gameData;
//    	}
//    	
//    	autoLengths.add(ru.toSwitchTime);
//    	autoCommands.add(ru.goForward);
//    	
//    	if(gameData == ru.pos) {
//    		
//    		autoLengths.add(ru.liftToSwitchTime);
//        	autoCommands.add(ru.goUpToSwitch);
//    		
//    		autoLengths.add(ru.turn90Time);
//    		autoCommands.add(ru.turn(-gameData));
//    		
//    		autoLengths.add(ru.sideToSwitchTime);
//    		autoCommands.add(ru.goSlowForward);
//    		
//    	}else {
//    		
//    		autoLengths.add(ru.pastSwitchTime);
//    		autoCommands.add(ru.goForward);
//    		
//    		autoLengths.add(ru.turn90Time);
//    		autoCommands.add(ru.turn(gameData));
//    		
//    		autoLengths.add(ru.crossBehindSwitchTime);
//    		autoCommands.add(ru.goForward);
//    		
//    		autoLengths.add(ru.turn90Time);
//    		autoCommands.add(ru.turn(gameData));
//    		
//    		autoLengths.add(ru.backOfSwitchApproachTime);
//    		autoCommands.add(ru.goSlowForward);
//    	}
//    	
//    	autoLengths.add(ru.spitOutTime);
//		autoCommands.add(ru.goSpitOut);
//		
//		autoLengths.add(ru.restTime);
//		autoCommands.add(ru.STOP);
    }
    
	public void autonomousPeriodic() {
		
		autoTime = System.currentTimeMillis() - autoStart;
		SmartDashboard.putNumber("auto time", autoTime);
		
		if(autoTime< 2500) {
			leftWheelTalon.set(PO, -.7);
			rightWheelTalon.set(PO, -.7);
		}
		
		
//		if(autoTime < 2250) {
//			leftWheelTalon.set(PO,.6);
//			rightWheelTalon.set(PO, .6);
//			//leftFeederTalon.set(0);
//			//rightFeederTalon.set(0);	
//		}else if(autoTime<2750) {
//			leftWheelTalon.set(PO,0);
//			rightWheelTalon.set(PO, 0);
////			leftFeederTalon.set(0);
////			rightFeederTalon.set(0);
//		}else if(autoTime < 4000) {	
//			leftWheelTalon.set(PO, 0);
//			rightWheelTalon.set(PO, 0);
//			liftTalon1.set(PO, .7);
//			liftTalon2.set(.7);
////			leftFeederTalon.set(0);
////			rightFeederTalon.set(0);
//		}else if(gameData == ru.pos) {
//			if(autoTime < 4750) {
//				leftWheelTalon.set(PO, gameData* -.6);
//				rightWheelTalon.set(PO, gameData* .6);
//				liftTalon1.set(PO, 0);
//				liftTalon2.set(0);
//				leftFeederTalon.set(0);
//				rightFeederTalon.set(0);
//			}else if(autoTime < 5500) {
//				leftWheelTalon.set(PO, .7);
//				rightWheelTalon.set(PO, .7);
//				liftTalon1.set(PO, 0);
//				liftTalon2.set(0);
//				leftFeederTalon.set(0);
//				rightFeederTalon.set(0);
//			}else {
//				leftWheelTalon.set(PO, 0);
//				rightWheelTalon.set(PO, 0);
//				liftTalon1.set(PO, .1);
//				liftTalon2.set(.1);
//				leftFeederTalon.set(1);
//				rightFeederTalon.set(1);
//			}	
//		}else {
//			leftWheelTalon.set(PO, 0);
//			rightWheelTalon.set(PO, 0);
//			liftTalon1.set(PO, .1);
//			liftTalon2.set(.1);
//			leftFeederTalon.set(0);
//			rightFeederTalon.set(0);
//		}
		
//		double timeSum = 0;
//		int index = 0;
//		
//		for(double length : autoLengths) {
//			if(autoTime < timeSum) break;
//			timeSum += length;
//			index++;
//		}
//		
//		double[] powers = autoCommands.get(index);
//		
//		leftWheelTalon.set(PO, powers[0]);
//		rightWheelTalon.set(PO, powers[1]);
//		liftTalon1.set(PO, powers[2]);
//		liftTalon2.set(powers[2]);
//		leftFeederTalon.set(powers[3]);
//		rightFeederTalon.set(-powers[3]);
	}
}