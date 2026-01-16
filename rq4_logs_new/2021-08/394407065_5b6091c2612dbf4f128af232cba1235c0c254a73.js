import React, { useState } from 'react';
import TodoForm from './TodoForm';
import { GrClose } from 'react-icons/gr';
import { FiEdit } from 'react-icons/fi';

function Todo({ todos, completeTodo, removeTodo, updatedTodo }) {

    const [edit, setEdit] = useState({
        id: null,
        value: '',
    });

    const submitUpdate = value => {
        updatedTodo(edit.id, value);
        setEdit({ 
            id: null, 
            value: ''
        });
    };

    if (edit.id) {
        return <TodoForm edit={edit} onSubmit={submitUpdate} />;
    }


    return todos.map((todo, index) => (
        <div className={todo.isComplete ? 'todo-row complete' : 'todo-row'} key={index}>
          <div key={todo.id} onClick={() => completeTodo(todo.id)}>
              {todo.text}
          </div>  

        <div className="icons">
            <GrClose 
                onClick={() => removeTodo(todo.id)}
                className="delete-icon"
            />
            <FiEdit 
                onClick={() => setEdit({ id: todo.id, value: todo.text })}
                className="edit-icon"
            />
        </div>

        </div> 
    ))
}

export default Todo