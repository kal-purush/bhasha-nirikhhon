/**
 * AddCommand Class extends the abstract Command class
 * Called when items should be ADDED to tasks
 * @author Kane Quah
 * @version 1.0
 * @since 09/19
 */
public class BadCommand extends Command {
    BadCommand(String command, String arguments){
    }

    /**
     * default execute overwritten to throw errors
     * @param tasks TasksList Object being used currently
     * @param ui UI in charge of printing messages
     * @param storage Storage in charge of loading and saving files
     * @throws DukeException DukeException instantly thrown
     */
    public void execute(Tasklist tasks, UI ui, Storage storage) throws DukeException {
        throw new DukeException("☹ OOPS!!! I'm sorry, but I don't know what that means :-(");
    }

}