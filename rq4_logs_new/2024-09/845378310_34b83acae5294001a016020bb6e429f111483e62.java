package sunny;

import java.util.List;

public class DeleteCommand extends Command{
    private String s = "";
    @Override
    public String runCommand(List<Task> ls, String m) {
        int i2 = Integer.parseInt(m);
        String s = ls.get(i2 - 1).toString();
        ls.remove(i2 -1);
        return line + "\n     "
                + "Got it! removed the task: \n     "
                + s + "\n     "
                + String.format("Now you have %h tasks in the list \n", ls.size()) + line;
    }
}