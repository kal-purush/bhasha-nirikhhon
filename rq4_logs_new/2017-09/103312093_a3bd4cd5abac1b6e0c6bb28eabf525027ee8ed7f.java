package umm3601.todo;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;

public class FilterTodosFromDB {

  Database db;
  Map<String, String[]> queryParams = new HashMap<>();

  @Before
  public void setup() throws IOException {
    db = new Database("src/main/data/todos.json");
  }

  @Test
  public void listTodosWitOwnerFilter() {
    queryParams.put("owner", new String[] {"Fry"});
    Todo[] filteredTodos = db.listTodos(queryParams);
    assertEquals("Incorrect number of todos", 61, filteredTodos.length);
  }

  @Test
  public void listTodosWitStatusFilter() {
    queryParams.put("status", new String[] {"true"});
    Todo[] filteredTodos = db.listTodos(queryParams);
    assertEquals("Incorrect number of todos", 143, filteredTodos.length);
  }

  @Test
  public void listTodosWitStatusFilter2() {
    queryParams.put("status", new String[] {"false"});
    Todo[] filteredTodos = db.listTodos(queryParams);
    assertEquals("Incorrect number of todos", 157, filteredTodos.length);
  }

  @Test
  public void listTodosWithContainsFilter() {
    queryParams.put("contains", new String[] {"magna tempor"});
    Todo[] filteredTodos = db.listTodos(queryParams);
    assertEquals("Incorrect number of todos", 2, filteredTodos.length);
  }

  @Test
  public void listTodosWithContainsFilter2() {
    queryParams.put("contains", new String[] {"reprehenderit fugiat"});
    Todo[] filteredTodos = db.listTodos(queryParams);
    assertEquals("Incorrect number of todos", 3, filteredTodos.length);
  }

  @Test
  public void listTodosWithContainsFilter3() {
    queryParams.put("contains", new String[] {"Laborum"});
    Todo[] filteredTodos = db.listTodos(queryParams);
    assertEquals("Incorrect number of todos", 5, filteredTodos.length);
  }

  @Test
  public void listTodosWithCategoryFilter() {
    queryParams.put("category", new String[] {"homework"});
    Todo[] filteredTodos = db.listTodos(queryParams);
    assertEquals("Incorrect number of todos", 79, filteredTodos.length);
  }

  @Test
  public void listTodosWithTwoFilter() {
    queryParams.put("category", new String[] {"homework"});
    queryParams.put("status", new String[] {"true"});
    Todo[] filteredTodos = db.listTodos(queryParams);
    assertEquals("Incorrect number of todos", 39, filteredTodos.length);
  }

}