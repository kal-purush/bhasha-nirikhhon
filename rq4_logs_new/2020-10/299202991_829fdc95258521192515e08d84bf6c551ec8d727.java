package seedu.commands;

import seedu.data.Model;
import seedu.exceptions.EmptyDataStackException;
import seedu.exceptions.InvalidDatetimeException;
import seedu.exceptions.InvalidPriorityException;
import seedu.exceptions.InvalidTaskNumberException;
import seedu.exceptions.MaxNumTaskException;
import seedu.parser.Parser;
import seedu.ui.Ui;


public class LogicManager {

    private Parser parser;
    private final Ui ui;
    private final Model model;
    private boolean isExit;

    public LogicManager(Ui ui, Model model) {
        this.ui = ui;
        this.model = model;
        init();
    }

    private void init() {
        parser = new Parser();
        isExit = false;
    }

    public void run() {
        while (!isExit) {
            try {
                String userInput = ui.getUserInput();
                assert userInput != null : "null input";
                Command command = parser.processRaw(userInput);
                assert command != null : "null command";
                CommandResult result = executeCommand(command);
                assert result != null : "null command result";
                isExit = ui.interpretCommandResult(result);
            } catch (Exception e) {
                ui.showException(e);
            }
        }
    }

    private CommandResult executeCommand(Command cmd)
        throws MaxNumTaskException, InvalidTaskNumberException, InvalidDatetimeException,
        InvalidPriorityException, EmptyDataStackException {
        if (cmd instanceof ReadOnlyCommand) {
            return cmd.execute(model.getTaskMap());
        } else if (cmd instanceof ModificationCommand) {
            return cmd.execute(model);
        } else {
            return cmd.execute();
        }
    }
}