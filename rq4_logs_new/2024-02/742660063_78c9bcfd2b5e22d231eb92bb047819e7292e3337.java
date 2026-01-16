package frc.robot;

import edu.wpi.first.networktables.NetworkTable;
import edu.wpi.first.networktables.NetworkTableInstance;

public class limelightData {

    NetworkTable limelightTable = NetworkTableInstance.getDefault().getTable("limelight");

        public static double needRotate;
        public static double needStrafe;
        public static double needTranslate;

        public static double XC;
        public static double YC;
        public static double ZC;
        public static double targetRoll;
        public static double targetPitch;
        public static double targetYaw;

    


    public void calculate(){
        needStrafe = 0;
        needTranslate = 0;

        double[] targetPose = limelightTable.getEntry("targetpose_cameraspace").getDoubleArray(new double[6]);

        double targetX = limelightTable.getEntry("tx").getDouble(0.0);
        //double targetY = limelightTable.getEntry("ty").getDouble(0.0);
        //double targetA = limelightTable.getEntry("ta").getDouble(0.0);
        //double targetS = limelightTable.getEntry("ts").getDouble(0.0);

        double XC = targetPose[0];
        double YC = targetPose[1];
        double ZC = targetPose[2];
        //double targetRoll = targetPose[3];
        //double targetPitch = targetPose[4];
        //double targetYaw = targetPose[5]; 
        if(!(targetX < 1)){
            needRotate = targetX;
        }

       System.out.println(Math.sqrt((XC*XC)+(YC*YC)+(ZC*ZC)));
       // System.out.println("Roll: " + targetRoll + "   Pitch: " + targetPitch + "   Yaw: " + targetYaw);
        //System.out.println("X: " + targetXC + "   Y: " + targetYC + "   Z: " + targetZC);
        //System.out.println("Distance to Tag: " + distance);




    }
}