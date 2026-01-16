/*----------------------------------------------------------------------------*/
/* Copyright (c) 2018-2019 FIRST. All Rights Reserved.                        */
/* Open Source Software - may be modified and shared by FRC teams. The code   */
/* must be accompanied by the FIRST BSD license file in the root directory of */
/* the project.                                                               */
/*----------------------------------------------------------------------------*/

package frc.robot;

public final class Constants {

  // === SHOOTER === //
  public static final int DEFAULT_TARGET_RPM = 5000;
  public static final int SHORT_DISTANCE_RPM = 2000; // TODO: Change the numbers
  public static final int LONG_DIST_RPM = 5500; // TODO: Change the numbers
  public static final int SHOOTER_REV_TIME = 3000; // in ms

  
  // === VISION TRACKING CONSTANTS === //
  public static final int HORIZONTAL_FOV = 60;
  public static final int VERTICAL_FOV = 40;
  public static final double HEIGHT_OF_CAM = 24.7401; // IN INCHES


  // === MOTOR PORTS === //

  // Drivetrain
  public static final int FRONT_LEFT_DRIVE_PORT = 3; // 2
  public static final int BACK_LEFT_DRIVE_PORT = 0; // 1
  public static final int FRONT_RIGHT_DRIVE_PORT = 2; // 0
  public static final int BACK_RIGHT_DRIVE_PORT = 1; // 3

  // Intake
  public static final int WHEEL_INTAKE_PORT = 6;

  // Climber
  public static final int LIFTER_MOTOR = 1;
  public static final int GEAR_MOTOR1 = 7;
  public static final int GEAR_MOTOR2 = 8;

  // Shooter
  public static final int SHOOTER_MOTOR_1 = 4;
  public static final int SHOOTER_MOTOR_2 = 5;

  // Storage
  public static final int STORAGE_GATE_MOTOR_PORT = 9;


  // === PISTON PORTS === //

  // Intake
  public static final int INTAKE_PISTON_PORT_1 = 0;
  public static final int INTAKE_PISTON_PORT_2 = INTAKE_PISTON_PORT_1 + 1;

  public static final int SHOOTER_PISTON_PORT_1 = 2;
  public static final int SHOOTER_PISTON_PORT_2 = SHOOTER_PISTON_PORT_1 + 1;


  // === SENSOR PORTS === //

  // STORAGE LIMIT SWITCHES
  public static final int STORAGE_INTAKE_SWITCH_PORT = 0;
  public static final int STORAGE_BALL_SWITCH_PORT = 1;
  public static final int STORAGE_OVERRIDE_SWITCH_PORT = 2;

  // SHOOTER ENCODER
  public static final int SHOOTER_ENCODER_1 = 0;

  // === SPEED CONSTANTS === //

  // INTAKE
  public static final double DEPLOY_INTAKE_SPEED = 0.5;
  public static final double WHEEL_INTAKE_SPEED = -0.8;

  // STORAGE
  public static final double FEED_SPEED = -0.4;

  // CLIMBER
  public static final double LIFTER_SPEED = 1;
  public static final double LIFTER_SPEED_REVERSE = -1;
  public static final double GEAR_SPEED = 0.75;


  // == JOYSTICK CONSTANTS == //

  // Controllers
  public static final int DRIVER_CONTROLLER = 0;
  public static final int OPERATOR_CONTROLLER = 1;

  // Sticks (for xbox controller)
  public static final int HORIZ_AXIS_LEFT = 0;
  public static final int FORWARD_AXIS_LEFT = 1;
  public static final int HORIZ_AXIS_RIGHT = 4;
  public static final int FORWARD_AXIS_RIGHT = 5;

  // Axes
  public static final int LEFT_TRIGGER_AXIS = 2;
  public static final int RIGHT_TRIGGER_AXIS = 3;

  // Buttons (for xbox controller)
  public static final int BUTTON_A = 1;
  public static final int BUTTON_B = 2;
  public static final int BUTTON_X = 3;
  public static final int BUTTON_Y = 4;
  public static final int LEFT_BUMPER = 5;
  public static final int RIGHT_BUMPER = 6;
  public static final int BACK_BUTTON = 7;
  public static final int START_BUTTON = 8;
  public static final int LEFT_STICK_BUTTON = 9;
  public static final int RIGHT_STICK_BUTTON = 10;


  // === PIXY SIGs === //

  public static enum Signature {

    GOAL_BOTTOM_LINE(2), POWER_CELL(-1);

    int sig = -1;

    Signature(int sig) {
      this.sig = sig;
    }

    public int value() {
      return sig;
    }
  }

}