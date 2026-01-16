package ru.practicum.category.controllers;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import ru.practicum.category.dto.CategoryDto;
import ru.practicum.category.dto.CreateCategoryDto;
import ru.practicum.category.dto.UpdateCategoryDto;
import ru.practicum.category.services.AdminCategoryService;

@RestController
@RequestMapping(path = "/admin/categories")
@RequiredArgsConstructor
@Validated
public class AdminCategoryController {
    private final AdminCategoryService adminCategoryService;

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public CategoryDto create(@Valid @RequestBody CreateCategoryDto categoryDto) {
        return adminCategoryService.create(categoryDto);
    }

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteCategory(@PathVariable Long id) {
        adminCategoryService.deleteCategory(id);
    }

    @PatchMapping("/{id}")
    public CategoryDto update(@PathVariable Long id, @Valid @RequestBody UpdateCategoryDto categoryDto) {
        return adminCategoryService.update(id, categoryDto);
    }
}