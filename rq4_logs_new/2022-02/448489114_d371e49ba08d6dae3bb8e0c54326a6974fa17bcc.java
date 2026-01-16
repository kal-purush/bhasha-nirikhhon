package duke.command;

import duke.exception.DukeException;
import duke.manager.Storage;
import duke.manager.TaskList;
import duke.task.Deadline;
import duke.task.Task;

/**
 * Represents a command that will update a Deadline Task to the TaskList upon execution.
 */
public class UpdateDeadlineCommand extends Command {
    private int taskNo;
    private String taskName;
    private String by;

    /**
     * A constructor to store the taskNo and the details to be updated.
     *
     * @param arguments The arguments that may or may not contain the details and date.
     * @throws DukeException If a task number was not provided or if there is nothing to update.
     */
    public UpdateDeadlineCommand(String arguments) throws DukeException {
        super();
        try {
            this.taskNo = Integer.parseInt(arguments.split(" ")[2]) - 1;
        } catch (NumberFormatException e) {
            throw new DukeException("OOPS!!! Please enter a task number!");
        }
        if (arguments.contains("/details") && arguments.split("/details ").length > 1) {
            String detailsAndDate = arguments.split("/details")[1]; // Extract the details.
            String detailsOnly = detailsAndDate.indexOf("/date") == -1 // If there is no /date field.
                    ? detailsAndDate // Leave it as is.
                    : detailsAndDate.substring(0, detailsAndDate.indexOf("/date")); // Remove the date.
            this.taskName = detailsOnly.trim().length() == 0 ? null : " " + detailsOnly.trim();
        }
        if (arguments.contains("/date") && arguments.split("/date ").length > 1) {
            String detailsAndDate = arguments.split("/date")[1]; // Extract the details.
            String dateOnly = detailsAndDate.indexOf("/details") == -1 // If there is no /details field.
                    ? detailsAndDate // Leave it as it.
                    : detailsAndDate.substring(0, detailsAndDate.indexOf("/details")); // Remove the details.
            this.by = dateOnly.trim().length() == 0 ? null : dateOnly.trim(); // Don't assign if it is an empty string.
        }
        if (this.taskName == null && this.by == null) {
            throw new DukeException("Please enter details, date or both for me to update!");
        }
    }

    /**
     * Executes the command by updating the specified Deadline appropriately.
     *
     * @param taskList A TaskList that stores the tasks.
     * @param storage  A Storage object to handle saving of data.
     * @return A String which is Duke's response.
     * @throws DukeException If the task type provided was not correct or if there is an issue saving the tasks.
     */
    @Override
    public String execute(TaskList taskList, Storage storage) throws DukeException {
        Task task = taskList.getTask(taskNo);
        if (!(task instanceof Deadline)) {
            throw new DukeException("Please enter the correct task type!");
        }
        if (taskName == null) {
            taskList.update(taskNo, new Deadline(task.getTaskName(), by));
        } else if (by == null) {
            taskList.update(taskNo, new Deadline(taskName, ((Deadline) task).getDate()));
        } else {
            taskList.update(taskNo, new Deadline(taskName, by));
        }
        String response = "Got it. I've updated this task:" + "\n";
        response += taskList.getTask(taskNo) + "\n";
        try {
            storage.save(taskList);
        } catch (Exception e) {
            throw new DukeException(e.getMessage());
        }
        return response;
    }

    /**
     * Returns true if it is an exit command and false otherwise.
     *
     * @return A boolean.
     */
    @Override
    public boolean isExit() {
        return false;
    }

}