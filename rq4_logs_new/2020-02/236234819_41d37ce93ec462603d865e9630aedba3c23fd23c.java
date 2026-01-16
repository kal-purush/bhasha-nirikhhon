package frc.robot.subsystems;


public class AutoDrive extends Drive {
    public void turn(double degrees, double tolerance, Runnable callback) {
        (new Thread(() -> {
            setRotationalSetpoint(degrees);
            while(!autoOrient(tolerance)) {
                try {
                    Thread.sleep(20);
                }
                catch(InterruptedException e) {
                    System.out.println("TURN INTERRUPTED.");
                }
            }
            callback.run();
        })).start();
    }
    public void driveForwardByTime() {

    }
}