package seedu.commands;

import java.util.Timer;
import seedu.exceptions.InvalidTaskNumberException;
import seedu.task.Task;
import seedu.data.TaskMap;
import java.util.TimerTask;
import java.util.regex.Pattern;
import seedu.ui.Ui;
import java.util.Date;
import java.util.Calendar;
import static seedu.messages.Messages.REMINDER_MESSAGE;

public class Reminder extends ReadOnlyCommand {
    public static final String COMMAND_WORD = "reminder";
    public static final Pattern COMMAND_PATTERN = Pattern.compile("^reminder (?<key>\\d+)"
            + "( t/(?<t>\\d{4}))?$");
    private final Integer key;
    private final Integer time;
    private final Timer timer;

    public Reminder(String keyString, String timeString) throws InvalidTaskNumberException {
        timer = new Timer();
        if(timeString!=null) {
            this.time = Integer.parseInt(timeString);
        } else {
            this.time = null;
        }
        try {
            key = Integer.parseInt(keyString);
        } catch (NumberFormatException e) {
            throw new InvalidTaskNumberException();
        }
    }

    public CommandResult execute(TaskMap tasks) throws InvalidTaskNumberException {
        Task task = tasks.get(key);
        Calendar calendar = Calendar.getInstance();
        TaskMap taskReminder = new TaskMap();
        taskReminder.addTask(task);
        if (time != null) {
            calendar.set(task.getDate().getYear(), task.getDate().getMonthValue()-1,
                    task.getDate().getDayOfMonth(), (time / 100), (time % 100),0);
        } else {
            calendar.set(task.getDate().getYear(), task.getDate().getMonthValue()-1,
                    task.getDate().getDayOfMonth(), task.getStartTime().getHour()-1,
                    task.getStartTime().getMinute(),0);
        }
        Date date = calendar.getTime();
        if (task == null) {
            throw new InvalidTaskNumberException();
        }
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                System.out.println("Reminder, you have an upcoming task: ");
                Ui ui = new Ui();
                ui.displayTasks(taskReminder);
                timer.cancel();
            }
        }, date);
        return new CommandResult(REMINDER_MESSAGE);
    }

}