package seedu.commands;

import seedu.data.TaskList;
import seedu.exceptions.InvalidCommandException;
import seedu.exceptions.InvalidTaskNumberException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static seedu.messages.Messages.DELETE_MESSAGE;

public class Delete extends Command {
    public static final String COMMAND_WORD = "delete";
    private final int index;
    private static final Pattern COMMAND_PATTERN = Pattern.compile(
            "^delete (?<index>\\d+)"
                    + "( des/(?<description>(\\w+\\s*)+\\w*))?"
                    + "( d/(?<date>\\d{2}-\\d{2}-\\d{4}))?"
                    + "( st/(?<st>\\d{4}))?"
                    + "( et/(?<et>\\d{4}))?"
                    + "( p/(?<priority>\\d))?$");

    public Delete(String rawInput) throws InvalidCommandException {
        Matcher matcher = COMMAND_PATTERN.matcher(rawInput);
        if (matcher.find()) {
            index = Integer.parseInt(matcher.group("index"))-1;
        } else {
            throw new InvalidCommandException();
        }
    }

    @Override
    public CommandResult execute(TaskList tasks) throws InvalidTaskNumberException {
        if (index <= 0 || index >= tasks.size()) {
            throw new InvalidTaskNumberException();
        }
        tasks.delete(index);
        return new CommandResult(DELETE_MESSAGE);
    }
}