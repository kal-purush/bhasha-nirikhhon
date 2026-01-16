package frc.robot.sensors;
import edu.wpi.first.networktables.NetworkTable;
import edu.wpi.first.networktables.NetworkTableEntry;
import edu.wpi.first.networktables.NetworkTableInstance;

public class ShooterCamera {
    private NetworkTable cameraTable;
    public ShooterCamera(String camName) {
        NetworkTableInstance table = NetworkTableInstance.getDefault();
        cameraTable = table.getTable("chameleon-vision").getSubTable(camName);
    }
    public double getYaw() {
        return cameraTable.getEntry("targetYaw").getDouble(0.0);
    }
    public double getDistance() {
        return cameraTable.getEntry("distance").getDouble(0.0); //Double check this entry name
    }
}