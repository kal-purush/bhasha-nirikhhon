package umm3601.todo;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class LimitTodoListFromDB {

  Database db;
  Map<String, String[]> queryParams = new HashMap<>();

  @Before
  public void setup() throws IOException {
    db = new Database("src/main/data/todos.json");
  }

  @Test
  public void totalTodosCount5() throws IOException {
    queryParams.put("limit", new String[] {"5"});
    Todo[] limitedTodos = db.listTodos(queryParams);
    assertEquals("Incorrect total number of todos", 5, limitedTodos.length);
  }

  @Test
  public void totalTodosCount500() throws IOException {
    queryParams.put("limit", new String[] {"500"});
    Todo[] limitedTodos = db.listTodos(queryParams);
    assertEquals("Incorrect total number of todos", 300, limitedTodos.length);
  }

  @Test
  public void firstTodoInLimitedList5() throws IOException {
    queryParams.put("limit", new String[] {"5"});
    Todo[] limitedTodos = db.listTodos(queryParams);
    Todo firstTodo = limitedTodos[0];
    assertEquals("Incorrect id", "58895985a22c04e761776d54", firstTodo._id);
    assertEquals("Incorrect owner", "Blanche", firstTodo.owner);
    assertEquals("Incorrect status", false, firstTodo.status);
    assertEquals("Incorrect body", "In sunt ex non tempor cillum commodo amet incididunt anim qui commodo quis. Cillum non labore ex sint esse.", firstTodo.body);
    assertEquals("Incorrect category", "software design", firstTodo.category);
  }
}