package com.exxeta.expensemanagerms.controller.expenseController;

import com.exxeta.expenseservice.services.CategoryService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController()
@RequestMapping(path = "/categories/user/{userId}")
public class CategoryController {

    private final ObjectMapper mapper = new ObjectMapper();

    private final CategoryService categoryService;

    public CategoryController(CategoryService categoryService) {
        this.categoryService = categoryService;
    }

    @GetMapping
    public String getAllCategories(@PathVariable long userId) {
        try {
            return mapper.writeValueAsString(categoryService.getAllCategories(userId));
        } catch (Exception exception) {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, exception.getMessage());
        }
    }
}