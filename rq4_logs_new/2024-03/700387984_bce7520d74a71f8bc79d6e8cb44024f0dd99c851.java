package carleton.sysc4907.processing;

import carleton.sysc4907.command.*;
import carleton.sysc4907.command.args.*;

import java.util.HashMap;
import java.util.Map;

public class ExecutedCommandRunner {

    private Map<Class<?>, CommandFactory> commandFactories;

    public ExecutedCommandRunner(
            AddCommandFactory addCommandFactory,
            RemoveCommandFactory removeCommandFactory,
            MoveCommandFactory moveCommandFactory,
            ResizeCommandFactory resizeCommandFactory,
            EditTextCommandFactory editTextCommandFactory,
            ConnectorMovePointCommandFactory connectorMovePointCommandFactory,
            ConnectorSnapCommandFactory connectorSnapCommandFactory,
            ChangeConnectorStyleCommandFactory changeConnectorStyleCommandFactory) {

        this.commandFactories = new HashMap<>();
        commandFactories.put(AddCommandArgs.class, addCommandFactory);
        commandFactories.put(RemoveCommandArgs.class, removeCommandFactory);
        commandFactories.put(MoveCommandArgs.class, moveCommandFactory);
        commandFactories.put(ResizeCommandArgs.class, resizeCommandFactory);
        commandFactories.put(EditTextCommandArgs.class, editTextCommandFactory);
        commandFactories.put(ConnectorMovePointCommandArgs.class, connectorMovePointCommandFactory);
        commandFactories.put(ConnectorSnapCommandArgs.class, connectorSnapCommandFactory);
        commandFactories.put(ChangeConnectorStyleCommandArgs.class, changeConnectorStyleCommandFactory);
    }




    /**
     * Runs a list of commands to bring back the diagram to the state it was previously in.
     * @param commandArgsList the list of args objects for the previous commands run
     */
    public void runPreviousCommands(Object[] commandArgsList) {
        for (Object args : commandArgsList) {
            Class<?> argType = args.getClass();
            var factory = commandFactories.get(argType);
            if (factory == null) {
                throw new IllegalArgumentException("The given message did not correspond to a known type of command arguments.");
            }
            // Use remote commands to add them to the command list without transmitting them elsewhere
            Command<?> command = factory.createRemote((CommandArgs) argType.cast(args));
            command.execute();
        }
    }
}