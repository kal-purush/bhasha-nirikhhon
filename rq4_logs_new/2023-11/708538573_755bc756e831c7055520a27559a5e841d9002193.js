import React, { useState, useEffect } from 'react';
import './App.css';
import UndoNotification from './UndoNotification';

const App = () => {
  const [tasks, setTasks] = useState([]);
  const [inputValue, setInputValue] = useState('');
  const [editingTask, setEditingTask] = useState(null);
  const [deletedTask, setDeletedTask] = useState(null);

  useEffect(() => {
    if (deletedTask !== null) {
      const timeoutId = setTimeout(() => {
        setDeletedTask(null);
      }, 2000);

      return () => clearTimeout(timeoutId);
    }
  }, [deletedTask]);

  const handleInputChange = (e) => {
    setInputValue(e.target.value);
  };

  const handleKeyPress = (e) => {
    if (e.key === 'Enter' && inputValue.trim() !== '') {
      if (editingTask !== null) {
        editTask(editingTask, inputValue.trim());
        setEditingTask(null);
      } else {
        addTask(inputValue.trim());
      }
      setInputValue('');
    }
  };

  const addTask = (taskText) => {
    setTasks([...tasks, { text: taskText, id: Date.now() }]);
  };

  const deleteTask = (taskId) => {
    const deletedTask = tasks.find(task => task.id === taskId);
    setDeletedTask(deletedTask);

    const newTasks = tasks.filter(task => task.id !== taskId);
    setTasks(newTasks);
  };

  const undoDelete = () => {
    if (deletedTask !== null) {
      setTasks([...tasks, deletedTask]);
      setDeletedTask(null);
    }
  };

  const editTask = (taskId, newText) => {
    const newTasks = tasks.map(task => {
      if (task.id === taskId) {
        return { ...task, text: newText };
      }
      return task;
    });
    setTasks(newTasks);
  };

  const startEditing = (taskId) => {
    const taskToEdit = tasks.find(task => task.id === taskId);
    if (taskToEdit) {
      setEditingTask(taskId);
      setInputValue(taskToEdit.text);
    }
  };

  const checkTaskContent = (taskText) => {
    return taskText.toLowerCase().includes('estudar') || taskText.toLowerCase().includes('ler');
  };

  return (
    <div className="container">
      <h1>Minhas Tarefas</h1>
      <input
        type="text"
        value={inputValue}
        onChange={handleInputChange}
        onKeyPress={handleKeyPress}
        placeholder="Adicionar uma tarefa"
      />
      <ul>
        {tasks.map((task) => (
          <li key={task.id} className={checkTaskContent(task.text) ? 'blue-background' : ''}>
            {editingTask === task.id ? (
              <input
                type="text"
                value={inputValue}
                onChange={handleInputChange}
                onKeyPress={(e) => {
                  if (e.key === 'Enter') {
                    handleKeyPress(e);
                  }
                }}
              />
            ) : (
              <span onClick={() => startEditing(task.id)}>{task.text}</span>
            )}
            <span className="delete-icon" onClick={() => deleteTask(task.id)}>
              🗑️
            </span>
          </li>
        ))}
      </ul>
      {deletedTask && (
        <UndoNotification onClick={undoDelete} />
      )}
    </div>
  );
};

export default App;