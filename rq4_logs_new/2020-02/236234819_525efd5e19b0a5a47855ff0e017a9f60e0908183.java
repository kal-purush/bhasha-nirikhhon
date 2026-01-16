package frc.robot.commands;
import frc.robot.subsystems.Drive;

public class DriveInRegularPolygonSequence extends Command {
    private TurnByDegrees[] turn;
    private DriveByTime[] driveByTime;
    public DriveInRegularPolygonSequence(Drive drive, double timeInSeconds, double speed, 
                                    double acceptableError, int numVertices, String name, 
                                    int periodInMillis, boolean verbose) {
        super(name, periodInMillis, verbose);
        driveByTime = new DriveByTime[4];
        for(int i = 0; i < numVertices; i++) {
            driveByTime[i] = new DriveByTime(drive, timeInSeconds, speed, name + "-drive-" + i, periodInMillis);
        }
        turn = new TurnByDegrees[4];
        for(int i = 0; i < numVertices; i++) {
            turn[i] = new TurnByDegrees(drive, 360 / numVertices, acceptableError, name + "-turn-" + i, periodInMillis);
        }
    }
    protected void onStart() {
        super.onStart();
        for(int i = 0; i < turn.length; i++) {
            driveByTime[i].run();
            try {
                driveByTime[i].join();
            }
            catch(InterruptedException e) {
                onInterrupted();
            }
            turn[i].run();
            try {
                turn[i].join();
            }
            catch(InterruptedException e) {
                onInterrupted();
            }
        }
    }
    protected void whileRunning() {
        //Do nothing, because this Command has no periodic component
    }
    protected boolean isFinished() {
        //As soon as onStart() is finished, the entire Command is finished.
        return true;
    }
}