
import React, { useState } from 'react';
import './App.css';
import AddTask from './Components/Add Task/AddTask';
import ToDo from './Components/ToDoList/ToDo';
import UpdateTask from './Components/Update Task/UpdateTask';




function App() {
  // todo list state
  const [todo, setTodo] = useState([
    { "id": 1, "title": "Welcome To ToDo List 😊", "status": false },
    { "id": 2, "title": "Add Your Tasks Here ⬆️", "status": false },
    { "id": 3, "title": "Update Your Tasks ✒️ ", "status": false },
    { "id": 4, "title": "Complete Your Tasks 👌", "status": false },
  ])

  //temp state
  const [newTask, setNewTask] = useState('');
  const [updateData, setUpdateData] = useState('')
  //Get Task
  function getTask(e) {
    setNewTask(e.target.value)
  }
  //Add task
  function addTask(e) {
    e.preventDefault();
    if (newTask) {
      let taskId = todo.length + 1;
      let input = { id: taskId, title: newTask, status: false };
      setTodo([...todo, input]);
      setNewTask('')
    }
  }

  //Delete Task
  function deleteTask(id) {
    let newTasks = todo.filter(task => task.id !== id);
    setTodo(newTasks);

  }

  function markDone(id) {
    let newTasks = todo.map(task => {
      if (task.id === id) {
        return ({ ...task, status: !task.status })
      }
      return task;
    });
    setTodo(newTasks);
  }


  function cancelUpdate() {
    setUpdateData('')
  }

  function changeTask(e) {
    let input = {
      id: updateData.id,
      title: e.target.value,
      status: updateData.status ? true : false
    }
    setUpdateData(input);

  }
  function updateTask(e) {
    let filter = [...todo].filter(task => task.id !== updateData.id);
    let updatedObj = [...filter, updateData];
    setTodo(updatedObj);
    setUpdateData('');

  }

  return (
    <div className='container App'>
      <br /><br />
      <h2>To Do List</h2>
      <br /><br />

      {updateData && updateData ? (
        <UpdateTask updateData={updateData} updateTask={updateTask} changeTask={changeTask} cancelUpdate={cancelUpdate} />

      ) : (
        <AddTask newTask={newTask} getTask={getTask} addTask={addTask} />

      )
      }

      {/* Display Todos */}
      {todo && todo.length ? '' : 'No Tasks'}
      <ToDo todo={todo} markDone={markDone} deleteTask={deleteTask} setUpdateData={setUpdateData} />
    </div >

  );
}

export default App;