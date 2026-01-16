package pl.todolist.utils;

import pl.todolist.model.ToDoItem;

import java.sql.Date;
import java.util.List;

public class TestUtils {
    private static final ToDoItem ITEM_1 = new ToDoItem("description1", "details", new Date(100000000));
    private static final ToDoItem ITEM_2 = new ToDoItem("description2", "details", new Date(200000000));
    private static final ToDoItem ITEM_3 = new ToDoItem("description3", "details", new Date(300000000));
    private static final ToDoItem ITEM_4 = new ToDoItem("description4", "details", new Date(400000000));
    private static final ToDoItem ITEM_5 = new ToDoItem("description5", "details", new Date(500000000));

    public static List<ToDoItem> getDefaultClientsList() {
        return List.of(ITEM_1, ITEM_2, ITEM_3, ITEM_4, ITEM_5);
    }
}