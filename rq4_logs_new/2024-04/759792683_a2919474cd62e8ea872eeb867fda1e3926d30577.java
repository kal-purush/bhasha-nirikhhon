package seedu.address.model;

import javafx.collections.ObservableList;
import seedu.address.logic.commands.Command;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class CommandHistory implements ReadOnlyCommandHistory {

    private final CommandList commandList;

    public CommandHistory() {
        commandList = new CommandList();
    };

    public CommandHistory(ReadOnlyCommandHistory toBeCopied) {
        this();
        resetData(toBeCopied);
    }

    public void resetData(ReadOnlyCommandHistory newData) {
        requireNonNull(newData);

        setHistory(newData.getCommandHistory());
    }

    public void setHistory(List<Command> commands) {
        this.commandList.setList(commands);
    }

    public ObservableList<Command> getCommandHistory() {
        return commandList.asUnmodifiableObservableList();
    }
}