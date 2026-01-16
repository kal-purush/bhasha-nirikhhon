package sunny;

import java.util.List;
public class AddTaskCommand extends Command {
    @Override
    public String runCommand(List<Task> ls, String m) {
        try {
            Task t = TaskCreator.create(m + "|false");
            ls.add(t);
            return line + "\n     "
                    + "Got it! added the task: \n     "
                    + t + "\n     "
                    + String.format("Now you have %h tasks in the list \n", ls.size()) + line;
        } catch (Exception e) {
            return line + "\n      " + e + line;
        }

    }
}