package com.example.taskmanager.config.exception.classes.categoria;

import com.example.taskmanager.config.exception.classes.base.DuplicateException;
import lombok.Getter;

@Getter
public class CategoriaNameDuplicateException extends DuplicateException {
    private String categoria;

    public CategoriaNameDuplicateException(String message) {
        super(message);
    }

    public CategoriaNameDuplicateException(String categoria, String message) {
        super(message);
        this.categoria = categoria;
    }
}