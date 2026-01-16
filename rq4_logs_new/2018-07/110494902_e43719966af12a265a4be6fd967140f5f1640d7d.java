package org.snobot.lib.autonomous;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.snobot.lib.ASnobot;

import edu.wpi.first.wpilibj.command.Command;

/**
 * This class holds a map of all the autonomous command classes and allows
 * generic create of a command with just the command name.
 * 
 * @author Nora/Josh
 *
 */
public class AAutonomousCommandFactory<RobotType extends ASnobot>
{
    private static final Logger sLOGGER = LogManager.getLogger(AAutonomousCommandFactory.class);

    /**
     * This is the map that holds all our autonomous command create references
     * Note: For each autonomous command in org.snobot.commands put a entry in
     * the map.
     */
    private final Map<String, ICommandCreator<RobotType>> mCommandCreatorMap;

    /**
     * Constructor.
     */
    public AAutonomousCommandFactory()
    {
        mCommandCreatorMap = new HashMap<>();
    }

    protected final void registerCreator(String aCommandName, ICommandCreator<RobotType> aCreator)
    {
        mCommandCreatorMap.put(aCommandName, aCreator);
    }

    /**
     * It returns a command object after looking it up in the map by the command
     * name and then calling create command on the reference in the map.
     * 
     * @param aCommandName
     *            The name of the command
     * @param aCommandArgs
     *            The list of commands that are in the command text file.
     * @returns the command for that name or null if i doesn't exist.
     */
    public Command createCommand(String aCommandName, List<String> aCommandArgs, RobotType aSnobot)
    {
        Command newCommand;
        ICommandCreator<RobotType> commandCreator = mCommandCreatorMap.get(aCommandName);

        if (commandCreator == null)
        {
            newCommand = null;
            sLOGGER.log(Level.ERROR, "No creater registered for command '" + aCommandName + "'");
        }
        else
        {
            newCommand = commandCreator.createCommand(aCommandArgs, aSnobot);
        }
        return newCommand;
    }
}