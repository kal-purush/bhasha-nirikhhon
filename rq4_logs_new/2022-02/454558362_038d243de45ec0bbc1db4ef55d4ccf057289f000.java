package umm3601.todo;

/*
  Controller class for managing Todo objects, mirrors /users/UserController.java
*/

import io.javalin.http.Context;
import io.javalin.http.HttpCode;
import io.javalin.http.NotFoundResponse;

public class TodoController {

    private TodoDatabase database;

    /**
    * Construct a controller for users.
    * <p>
    * This loads the "database" of user info from a JSON file and stores that
    * internally so that (subsets of) users can be returned in response to
    * requests.
    *
    * @param database the `Database` containing user data
    */
    public TodoController(TodoDatabase database) {
        this.database = database;
    }
}