package com.example.myapplication.utilities;

import com.example.myapplication.model.Todo;

public interface TodoDAO {
    void saveTodo(Todo todo);

    void editTodo(Todo todo);
}