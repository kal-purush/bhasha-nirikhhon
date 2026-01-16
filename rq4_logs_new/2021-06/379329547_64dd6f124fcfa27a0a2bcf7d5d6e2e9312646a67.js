import './App.css';
import 'bootstrap/dist/css/bootstrap.min.css';

import React, { useState, useRef, useEffect } from 'react';
import TodoList from './components/TodoList';
import { v4 as uuidv4 } from 'uuid';


function App() {

  const [todos, setTodos] = useState([
    {
      id: 1,
      task: 'tarea 1',
      completed: false
    }
  ]);

  const toggleTodo = (id) => {
    const newTodos = [...todos]; // hacemos una copia del estado y lo almacenamos 
    const todo = newTodos.find((todo) => todo.id === id);
    todo.completed = !todo.completed // si estaba a true va a false y viseversa
    setTodos(newTodos); // alteramos el estado pasandole el new array
  }

  const handleClearAll = () => {
    const newTodos = todos.filter((todo) => !todo.completed); // hace una limpieza de los datos que no esten listo
    setTodos(newTodos);// alteramos el estados
  }

  const todoTaskRef = useRef();


  useEffect(() => {
    const storage = JSON.parse(localStorage.getItem("key"));
    if (storage) {
      setTodos(storage) // altera el estado
    }
  }, [])

  useEffect(() => {
    localStorage.setItem("key", JSON.stringify(todos));
  }, [todos])

  const handleTodoAdd = () => {
    const task = todoTaskRef.current.value;

    if (task === '') return;

    setTodos((prevTodos) => {
      return [...prevTodos, { id: uuidv4(), task, completed: false }]
    })

    // lo que hago ahora es resetiar el input
    todoTaskRef.current.value = null;
  }

  return (
    <div>
      <TodoList todos={todos} toggleTodo={toggleTodo} />
      <input ref={todoTaskRef} type="text" placeholder="Nueva Tarea" />
      <button onClick={handleTodoAdd}>+</button>
      <button onClick={handleClearAll}>v</button>
      <div style={{ marginLeft: '5px' }}>
        te quedan {todos.filter((todo) => !todo.completed).length}
      </div>
    </div>
  );
}

export default App;