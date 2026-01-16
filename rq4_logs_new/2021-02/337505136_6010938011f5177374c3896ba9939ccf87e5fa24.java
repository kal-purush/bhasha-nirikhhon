package umm3601.todo;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

public class FilterTodosByContainsFromDB {

  @Test
  public void filterUsersByAge() throws IOException {
    ToDoDatabase db = new ToDoDatabase("/todos.json");
    ToDo[] allTodos = db.listTodos(new HashMap<>());

    ToDo[] containsSunt = db.filterTodosByBody(allTodos, "In sunt");
    assertEquals(2, containsSunt.length, "Incorrect number of todos with 'In sunt'");

    ToDo[] containsSint = db.filterTodosByBody(allTodos, "sint");
    assertEquals(79, containsSint.length, "Incorrect number of todos with 'sint'");
  }

  @Test
  public void listUsersWithContainsFilter() throws IOException {
    ToDoDatabase db = new ToDoDatabase("/todos.json");
    Map<String, List<String>> queryParams = new HashMap<>();

    queryParams.put("contains", Arrays.asList(new String[] { "In sunt" }));
    ToDo[] allTodos = db.listTodos(queryParams);
    assertEquals(2, allTodos.length, "Incorrect number of todos with 'In sint'");

    queryParams.put("contains", Arrays.asList(new String[] { "sint" }));
    ToDo[] containsSint = db.listTodos(queryParams);
    assertEquals(79, containsSint.length, "Incorrect number of users with age 33");
  }
}