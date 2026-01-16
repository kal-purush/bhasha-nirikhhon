package Command;

import Storage.Storage;
import TaskList.TaskList;
import Ui.Ui;
import TaskList.Task;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

public class FindCommand extends Command{
    private String word;

    public FindCommand(String word) {
        this.word = word;
    }

    @Override
    public void execute(TaskList taskList, Ui ui, Storage storage) throws IOException {
        ui.showFindMessage(taskList.contains(word));
    }

    @Override
    public boolean isExit() {
        return false;
    }

}