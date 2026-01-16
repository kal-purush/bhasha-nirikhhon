import Dispatcher from "../cores/dispatcher";
import TodoConstants from "../constants/todo-constants";
import TodoSevice from "../services/todo-services";

class TodoActions {
  async getTodos() {
    let todos = await TodoSevice.getTodos();

    Dispatcher.dispatch({
      type: TodoConstants.TODO_LOAD_COMPLETE,
      todos: todos
    });
  }

  toggleTodo(todoId: string) {
    Dispatcher.dispatch({
      type: TodoConstants.TODO_TOGGLE,
      todoId
    });
  }

  filterTodosByText(filterText: string) {
    Dispatcher.dispatch({
      type: TodoConstants.TODO_FILTER_TEXT,
      filterText
    });
  }

  toggleAllTodos() {
    Dispatcher.dispatch({
      type: TodoConstants.TODO_TOGGLE_ALL,
    });
  }

  removeTodo(todoId) {
    Dispatcher.dispatch({
      type: TodoConstants.TODO_REMOVE,
      todoId
    });
  }
}

export default new TodoActions();