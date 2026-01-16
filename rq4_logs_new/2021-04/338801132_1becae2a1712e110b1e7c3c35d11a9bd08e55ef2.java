package seedu.taskify.logic.commands;

import static java.util.Objects.requireNonNull;
import static seedu.taskify.logic.commands.DeleteMultipleCommand.MESSAGE_SWITCH_TO_HOME;
import static seedu.taskify.logic.commands.util.DeleteUtil.deleteTasksFromModel;
import static seedu.taskify.logic.commands.util.DeleteUtil.generateSuccessMessage;
import static seedu.taskify.logic.commands.util.DeleteUtil.getTasksToDelete;

import java.util.List;
import java.util.Objects;

import seedu.taskify.logic.commands.exceptions.CommandException;
import seedu.taskify.logic.commands.util.DeleteUtil;
import seedu.taskify.model.Model;
import seedu.taskify.model.task.Status;
import seedu.taskify.model.task.Task;


/**
 * Deletes all tasks of a specific {@link Status}.
 */
public class DeleteByStatusCommand extends Command {

    private final Status statusOfTasksToDelete;

    /**
     * Creates a DeleteByStatusCommand that will delete all tasks of a specific {@link Status}
     * @param statusOfTasksToDelete the {@code Status} of tasks to delete by
     */
    public DeleteByStatusCommand(Status statusOfTasksToDelete) {
        this.statusOfTasksToDelete = statusOfTasksToDelete;
    }


    @Override
    public CommandResult execute(Model model) throws CommandException {
        requireNonNull(model);

        List<Task> lastShownList = model.getFilteredTaskList();
        List<Task> tasksToDelete;

        if (!CommandResult.isHomeTab()) {
            throw new CommandException(MESSAGE_SWITCH_TO_HOME);
        }

        tasksToDelete = getTasksToDelete(statusOfTasksToDelete, lastShownList);

        if (tasksToDelete.isEmpty()) {
            throw new CommandException(DeleteUtil.MESSAGE_NO_TASKS_OF_GIVEN_STATUS);
        }

        deleteTasksFromModel(model, tasksToDelete);
        return new CommandResult(generateSuccessMessage(tasksToDelete));
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o == null || getClass() != o.getClass()) {
            return false;
        } else {
            DeleteByStatusCommand cmd = (DeleteByStatusCommand) o;
            return statusOfTasksToDelete.equals(cmd.statusOfTasksToDelete);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(statusOfTasksToDelete);
    }
}