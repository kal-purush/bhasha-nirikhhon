import React, { useState, useEffect } from 'react';
import './App.css';

import Form from './components/Form/Form';
import TodoList from './components/TodoList/TodoList';

function App() {
    // States
    const [inputText, setInputText] = useState('');
    const [todos, setTodos] = useState([]);
    const [status, setStatus] = useState('all');
    const [filteredTodos, setFilteredTodos] = useState([]);

    // Run once
    useEffect(() => {
        getLocalTodos();
    }, []);
    // Use Effect
    useEffect(() => {
        filterHandler();
        saveLocalTodos();
    }, [todos, status]);

    //Functions
    const filterHandler = () => {
        switch (status) {
            case 'completed':
                setFilteredTodos(
                    todos.filter((todo) => todo.completed === true)
                );
                break;
            case 'uncompleted':
                setFilteredTodos(
                    todos.filter((todo) => todo.completed === false)
                );
                break;
            default:
                setFilteredTodos(todos);
                break;
        }
    };

    // Save to local
    const saveLocalTodos = () => {
        localStorage.setItem('todos', JSON.stringify(todos));
    };

    const getLocalTodos = () => {
        if (localStorage.getItem('todos') === null) {
            localStorage.setItem('todos', JSON.stringify([]));
        } else {
            let todoLocal = JSON.parse(localStorage.getItem('todos'));
            setTodos(todoLocal);
        }
    };

    return (
        <div className="App">
            <header>
                <h1>WORK TO-DOS</h1>
            </header>
            <Form
                inputText={inputText}
                setInputText={setInputText}
                todos={todos}
                setTodos={setTodos}
                setStatus={setStatus}
            />
            <TodoList
                todos={todos}
                setTodos={setTodos}
                filteredTodos={filteredTodos}
            />
        </div>
    );
}

export default App;