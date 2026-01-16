import React, { useState } from 'react'


const Todolist = () => {
  const [todos, setTodos] = useState([])
  const [todoName, setTodoName] = useState("")


  const addItem = () => {
    const newTodo = {
      id: 1 + Math.random(),
      value: todoName
    };

    const list = [...todos, newTodo]
    console.log("list",list)
    // list.push(newTodo);
    setTodos(list)
    setTodoName("")
  }

 const deleteTodo=(id)=> {
    const updatedList = todos.filter(todo => todo.id !== id);
console.log("updated",updatedList)
   setTodos(updatedList);
  }
  return (
    <div>
      <h1>My To Do List ♛</h1>
      <div className="container">
      Add a todo
      <br/>
      <br/>
      <input type='text' placeholder='enter to do here.....'
        value={todoName}
        onChange={e => setTodoName(e.target.value)} />
            {console.log("todo",todos)}
        {todos.map(todo=><div className="todos">{todo.value}<br/>
          <button className="btn btn-floating" onClick={() =>deleteTodo(todo.id)}>
            <i class="material-icons">x</i>
            </button>
        </div>)}
      <button onClick={() => addItem()}>Add</button>
      </div>
    </div>
  )
}

export default Todolist