import "./styles.css";

var todoList = {
  todos: ["siema", "placek", "ziemniak"],
  displayTodos: function() {
    console.log("My todos:", this.todos);
  },
  addTodo: function(todo) {
    this.todos.push(todo);
    this.displayTodos();
  },
  changeTodo: function(position, newValue) {
    this.todos[position] = newValue;
    this.displayTodos();
  },
  deleteTodo: function(position) {
    this.todos.splice(position, 1);
    this.displayTodos();
  }
};

todoList.changeTodo(0, "kartofle");
todoList.deleteTodo(2);