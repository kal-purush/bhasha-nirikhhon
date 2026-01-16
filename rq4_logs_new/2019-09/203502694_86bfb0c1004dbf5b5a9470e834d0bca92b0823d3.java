import Command.Command;
import Command.DeleteCommand;
import TaskList.TaskList;
import Ui.Ui;
import Storage.Storage;
import TaskList.Task;
import TaskList.ToDos;

import Ui.Ui;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;

public class DeleteCommandTest {
    @Test
    public void executeTest() throws IOException {
        ArrayList<Task> list = new ArrayList<>();
        list.add(new ToDos("test"));
        list.add(new ToDos("read book"));
        list.add(new ToDos("return book"));
        TaskList taskList = new TaskList(list);
        Storage storage = new Storage("/Users/jhchen/Documents/GitHub/duke/src/main/java/duke.txt");
        Ui ui = new Ui();
        Command c = new DeleteCommand(1);

        c.execute(taskList, ui, storage);
        assertEquals(2, taskList.list.size());
    }
}