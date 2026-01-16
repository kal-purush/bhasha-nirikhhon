package frc.robot.commands;

import edu.wpi.first.wpilibj2.command.Command;
import frc.robot.subsystems.LightsSusbsystem;

public class LightCommand extends Command{
    LightsSusbsystem lightsSusbsystem;
    private double setpoint;


    public LightCommand(LightsSusbsystem lightsSusbsystem, double setpoint){
        this.lightsSusbsystem = lightsSusbsystem;
        this.setpoint = setpoint;
        addRequirements(lightsSusbsystem);
    }

    @Override
    public void initialize() {
        lightsSusbsystem.setLight(setpoint);
    }

    @Override
    public boolean isFinished() {
        //System.out.println("jjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjj");
        return true;
    }

    @Override
    public void end(boolean interrupted) {
        //System.out.println("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh");
        //lightsSusbsystem.stop();
    
    }
    
}