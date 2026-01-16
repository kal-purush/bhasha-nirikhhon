package seedu.commands;

import seedu.data.TaskList;
import seedu.exceptions.InvalidCommandException;
import seedu.exceptions.InvalidPriorityException;
import seedu.task.Task;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static seedu.messages.Messages.ADD_MESSAGE;

public class Add extends Command {
    public static final String COMMAND_WORD = "add";
    // Default date: day that the task is created, default priority: 0 (low to high: 0 - 2)
    private static final Pattern COMMAND_PATTERN = Pattern.compile(
            "^(?<description>(\\w+\\s*)+\\w*)"
                    + "( d/(?<date>\\d{2}-\\d{2}-\\d{4}))?"
                    + "( t/(?<time>\\d{4}))?"
                    + "( p/(?<priority>\\d))?$");
    private final String description;
    private final String date;
    private final String time;
    private final String priority;

    public Add(String rawInput) throws InvalidCommandException {
        Matcher matcher = COMMAND_PATTERN.matcher(rawInput);
        if (matcher.find()) {
            description = matcher.group("description");
            date = matcher.group("date");
            time = matcher.group("time");
            priority = matcher.group("priority");
        } else {
            throw new InvalidCommandException();
        }
    }

    @Override
    public CommandResult execute(TaskList tasks) throws InvalidPriorityException {
        Task task = new Task(description, date, time, priority);
        tasks.addTask(task);
        return new CommandResult(ADD_MESSAGE, tasks, task);
    }
}