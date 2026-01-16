package frc.team4909.robot.sensors;

// import frc.team4909.robot.operator.*;
import edu.wpi.cscore.*;
import edu.wpi.first.wpilibj.command.InstantCommand;
import frc.team4909.robot.sensors.Stream;
import frc.team4909.robot.Robot;
import edu.wpi.first.wpilibj.smartdashboard.SmartDashboard;

public class ToggleCamera extends InstantCommand{
    public ToggleCamera(){
        SmartDashboard.putString("Cam Switch", "Toggle Constructor Called");

    }

    protected void execute(){ //part of superclass.
        Robot.stream.toggleCamera();
        SmartDashboard.putString("Switch", "Toggle Initialized");

    }

}