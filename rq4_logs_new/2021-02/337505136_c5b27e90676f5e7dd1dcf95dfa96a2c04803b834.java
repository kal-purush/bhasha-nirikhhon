package umm3601.todo;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

public class FilterTodosByCombinedFiltersFromDB {

  @Test
  public void listTodosWithCombinedFilters() throws IOException {
    ToDoDatabase db = new ToDoDatabase("/todos.json");
    Map<String, List<String>> queryParams = new HashMap<>();

    queryParams.put("contains", Arrays.asList(new String[] { "proident con" }));
    ToDo[] containsProident = db.listTodos(queryParams);
    assertEquals(3, containsProident.length, "Incorrect number of todos with 'proident con");

    queryParams.clear();
    queryParams.put("limit", Arrays.asList(new String[] { "2" }));
    ToDo[] limitTwoTodos = db.listTodos(queryParams);
    assertEquals(2, limitTwoTodos.length, "Incorrect number of todos listed, not 2");

    queryParams.clear();
    queryParams.put("contains", Arrays.asList(new String[] { "proident con" }));
    queryParams.put("limit", Arrays.asList(new String[] { "2" }));
    ToDo[] limitTwoProident = db.listTodos(queryParams);
    ToDo firstTodo = limitTwoProident[0];
    assertEquals("Workman", firstTodo.owner, "First owner should be Workman so we know both filters worked");
    assertEquals(2, limitTwoProident.length, "Incorrect number of todos with proident as the only 2");
  }

}