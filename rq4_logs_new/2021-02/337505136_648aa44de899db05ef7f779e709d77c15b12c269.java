package umm3601.todo;

import io.javalin.http.Context;
import io.javalin.http.NotFoundResponse;

/**
 * Controller that manages requests for info about todos.
 */
public class ToDoController {

  private ToDoDatabase database;

  /**
   * Construct a controller for todos.
   * <p>
   * This loads the "database" of todo info from a JSON file and stores that
   * internally so that (subsets of) todos can be returned in response to
   * requests.
   *
   * @param todoDatabase the `Database` containing todo data
   */
  public ToDoController(ToDoDatabase database) {
    this.database = database;
  }

  /**
   * Get the single todo specified by the `id` parameter in the request.
   *
   * @param ctx a Javalin HTTP context
   */
  public void getTodo(Context ctx) {
    String id = ctx.pathParam("id", String.class).get();
    ToDo todo = database.getToDo(id);
    if (todo != null) {
      ctx.json(todo);
      ctx.status(201);
    } else {
      throw new NotFoundResponse("No todo with id " + id + " was found.");
    }
  }

  /**
   * Get a JSON response with a list of all the todos in the "database".
   *
   * @param ctx a Javalin HTTP context
   */
  public void getTodos(Context ctx) {
    ToDo[] todos = database.listTodos(ctx.queryParamMap());
    ctx.json(todos);
  }

}