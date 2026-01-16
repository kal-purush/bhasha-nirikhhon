package frc.team4909.robot;

public class RobotConstants {
  /* Intake */
  public static final double cargoIntakeInSpeed = 1.0;
  public static final double cargoIntakeOutSpeed = -1.0;

  /* Sensor Threshold derived by testing the minimum voltage 
   * readouts when the ball is placed LEFT, RIGHT, and CENTER.
   * This value should then be compared to when there is no cargo
   * to ensure that the values do not overlap. The distinguishing value 
   * is then denoted as the treshold.
   */
  public static final double irSensorThreshold = 1.7;
}