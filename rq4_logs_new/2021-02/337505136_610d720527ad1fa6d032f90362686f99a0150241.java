package umm3601.todo;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;

import org.junit.jupiter.api.Test;

/**
 * Tests umm3601.todo.ToDoDatabase getTodo functionality
 */
public class GetTodoByIDFromDB {

  @Test
  public void getRoberta() throws IOException {
    ToDoDatabase db = new ToDoDatabase("/todos.json");
    ToDo user = db.getToDo("58895985ee196f2401e8c52a");
    assertEquals("Roberta", user.owner, "Incorrect name");
  }

  @Test
  public void getFry() throws IOException {
    ToDoDatabase db = new ToDoDatabase("/todos.json");
    ToDo user = db.getToDo("588959852d1d1f8a823ab71e");
    assertEquals("Fry", user.owner, "Incorrect name");
  }

}