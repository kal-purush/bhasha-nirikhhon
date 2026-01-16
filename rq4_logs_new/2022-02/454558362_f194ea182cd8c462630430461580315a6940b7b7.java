package umm3601.todo;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

/*
  Tests sorting todos alphabetically by body contents.
*/
@SuppressWarnings({ "MagicNumber" })
public class SortTodosByBody {

  @Test
  public void testBodySort() throws IOException {
    TodoDatabase db = new TodoDatabase("/todos.json");
    Map<String, List<String>> queryParams = new LinkedHashMap<>();

    queryParams.put("orderBy", Arrays.asList(new String[] {"body"}));
    Todo[] sortedTodos = db.sortTodosByBody();
    assertEquals("Ad sint incididunt", sortedTodos[0].body.substring(0, 18));
  }

}