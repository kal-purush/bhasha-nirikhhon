
package frc.robot.subsystems;

import edu.wpi.first.networktables.NetworkTable;
import edu.wpi.first.networktables.NetworkTableInstance;

public class LimelightTracking {

    // Define the desired April tag ID
    private static final int DESIRED_TAG_ID = 16; 

   
    public void startTracking(){
            // Get the default instance of NetworkTables
        NetworkTableInstance ntInst = NetworkTableInstance.getDefault();

        // Connect to the Limelight network table
        NetworkTable limelightTable = ntInst.getTable("limelight");

        // Subscribe to the "camtran" table to get camera pose data
        NetworkTable camPoseTable = limelightTable.getSubTable("camtran");

        // Continuously read and process pose data
        while (true) {
            double[] camPose = camPoseTable.getEntry("camtran").getDoubleArray(new double[6]);
            
            // Check if the first element of camPose array is the desired tag ID
            if ((int) camPose[0] != 0) {
                // Print pose data only when the desired tag is detected
                double x = camPose[1];
                double y = camPose[2];
                double z = camPose[3];
                double yaw = camPose[4];
                double pitch = camPose[5];
                double roll = camPose[6];
    
                System.out.println("April Tag ID " + camPose[0] + " detected:");
                System.out.println("Camera Pose: X=" + x + ", Y=" + y + ", Z=" + z + ", Yaw=" + yaw + ", Pitch=" + pitch + ", Roll=" + roll);
            }
            else{
               //System.out.println("No AprilTag detected"); 
            }

            // Add delay to avoid spamming the network tables
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}