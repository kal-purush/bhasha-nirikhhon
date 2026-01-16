package com.ctv.todos.repository;

import java.util.List;

import org.springframework.data.repository.CrudRepository;

import com.ctv.todos.model.Todo;

public interface TodoRepository extends CrudRepository<Todo, Long> {

	List<Todo> findByDoneTrue();
}