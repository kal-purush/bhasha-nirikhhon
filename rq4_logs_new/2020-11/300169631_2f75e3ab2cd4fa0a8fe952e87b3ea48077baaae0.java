package seedu.duke.commands;

import seedu.duke.model.ListType;
import seedu.duke.model.Model;
import seedu.duke.model.itemlist.TaskList;

import java.util.HashMap;

public class CommandTest {
    protected static final String TEST_DESCRIPTION = "test description";
    protected HashMap<String, String> argumentsMap;
    protected Model model;
    protected TaskList tasks;

    /**
     * Resets the fields for each test case.
     */
    void resetFields() {
        model = new Model();
        argumentsMap = new HashMap<>();
        tasks = (TaskList) model.getList(ListType.TASK_LIST);
    }
}