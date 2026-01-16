package frc.robot.commands;

import edu.wpi.first.wpilibj2.command.Command;
import edu.wpi.first.wpilibj2.command.ParallelCommandGroup;
import frc.robot.subsystems.IndexSubsystem;
import frc.robot.subsystems.IntakeSubsystem;

public class CommandGroups {


    public static Command intakeFull(IntakeSubsystem intakeSubsystem, IndexSubsystem indexSubsystem) {


        return new ParallelCommandGroup(

                new IntakeCommand(intakeSubsystem),
                new IndexCommand(indexSubsystem)

        );

    }

    public static Command outakeFull(IntakeSubsystem intakeSubsystem, IndexSubsystem indexSubsystem) {

        return new ParallelCommandGroup (


            new OutakeCommand(intakeSubsystem),
            new OutdexCommand(indexSubsystem)


        );

    }

}