package com.jevin.jmartapi.controller;

import com.jevin.jmartapi.model.Category;
import com.jevin.jmartapi.repository.CategoryRepo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/categories")
public class CategoryController {

    @Autowired
    CategoryRepo repo;

    @GetMapping("/{id}")
    public Optional<Category> get(@PathVariable int id) {
        return repo.findById(id);
    }

    @GetMapping
    public List<Category> getAll() {
        return repo.findAll();
    }

}