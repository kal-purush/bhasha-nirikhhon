 type Todo = {
    text: string,
    complete: boolean;
  };

  type ToggleTodo =  (selectedTodo: Todo) => void;

  type AddTodo = (newTodo: string) => void;

  type RemoveTodo = (removeTodo: string) => void;

  /* Types die automatische eruit worden gehaald */