package frc.robot.commands;

import edu.wpi.first.wpilibj2.command.InstantCommand;
import frc.robot.subsystems.Shooter;

public class SetShooterSpeedByAprilTag extends InstantCommand {
    private final Shooter shooter;

    public SetShooterSpeedByAprilTag(Shooter shooter) {
        this.shooter = shooter;
    }

    @Override
    public void initialize() {
        int detectedTagID = shooter.getDetectedAprilTagID();
        double speed;
        System.out.println("SHOOTER INITALIZED");

        switch (detectedTagID) {
            case 6:  // Adjust speed for BLUE AMP 
                speed = 0.175;  // Example speed for Tag 1
                System.out.println("\n\n BLUE AMP DETECTED");
                break;
            case 7:  // Adjust speed for BLUE SPEAKER
                speed = 1.0;   // Example speed for Tag 2
                System.out.println("\n\n BLUE Speaker DETECTED");
                break;
            case 5:  // Adjust speed for RED AMP
                speed = 0.175;  // Example speed for Tag 1
                System.out.println("\n\n RED AMP DETECTED");
                break;
            case 4:  // Adjust speed for RED SPEAKER
                speed = 1.0;   // Example speed for Tag 2
                System.out.println("\n\n RED Speaker DETECTED");
                break;
            default:
                speed = 0.0;   // Stop shooter if no tag detected
        }

        shooter.runShooterAtSpeed(speed);
    }
}
