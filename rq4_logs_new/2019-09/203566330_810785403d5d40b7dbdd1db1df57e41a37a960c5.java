public class ModCommand extends Command {
    private String command;
    private String input;
    ModCommand(String command, String input){
        this.command = command;
        this.input = input;
    }
    @Override
    public void execute(Tasklist tasks, UI ui, Storage storage) throws DukeException {
        if(this.command.matches("done")){
            tasks.markDone(this.input);
        }
        else if(this.command.matches("delete"))
        {
            tasks.banishDelete(this.input);
        }
    }

}