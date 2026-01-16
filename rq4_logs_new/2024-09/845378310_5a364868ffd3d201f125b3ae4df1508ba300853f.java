package sunny;

import java.time.LocalDate;
import java.util.List;

/**
 * Snooze current tasks in the task list
 */
public class SnoozeCommand extends Command {
    private String s = "";
    @Override
    public String runCommand(List<Task> ls, String m) {
        Parser p = new Parser(m, "/by");
        int i = Integer.parseInt(p.getFirstHalf().trim());
        LocalDate d = LocalDate.parse(p.getSecondHalf().trim());
        Task t = ls.get(i - 1);
        t.changeTimeline(d);
        return "     Snoozed the task: \n     " + t;
    }
}