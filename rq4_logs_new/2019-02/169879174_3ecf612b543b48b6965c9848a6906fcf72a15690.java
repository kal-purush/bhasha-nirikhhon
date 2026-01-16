package planmysem.commands;

/**
 * Terminates the program.
 */
public class ExitCommandP extends CommandP {

    public static final String COMMAND_WORD = "exit";

    public static final String MESSAGE_USAGE = COMMAND_WORD + ":\n" + "Exits the program.\n\t"
            + "Example: " + COMMAND_WORD;
    public static final String MESSAGE_EXIT_ACKNOWEDGEMENT = "Exiting PlanMySem as requested ...";

    @Override
    public CommandResultP execute() {
        return new CommandResultP(MESSAGE_EXIT_ACKNOWEDGEMENT);
    }

}