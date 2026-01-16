package ru.practicum.ewm.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import ru.practicum.ewm.entity.dto.CategoryDto;
import ru.practicum.ewm.entity.dto.NewCategoryDto;
import ru.practicum.ewm.error.exeptions.BadRequestException;
import ru.practicum.ewm.service.CategoryService;

import javax.validation.Valid;

@Slf4j
@RestController
@RequestMapping(path = "/admin/categories")
@Validated
public class AdminCategoriesController {

    private final CategoryService categoryService;

    @Autowired
    public AdminCategoriesController(CategoryService categoryService) {
        this.categoryService = categoryService;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public CategoryDto createCategory(@Valid @RequestBody NewCategoryDto newCategoryDto) {
        if (newCategoryDto.getName() == null) {
            throw new BadRequestException("NewCategoryDto.name is empty");
        }
        return categoryService.createCategory(newCategoryDto);
    }

    @PatchMapping("/{catId}")
    public CategoryDto patchCategory(@Valid @RequestBody CategoryDto categoryDto, @PathVariable Long catId) {
        if (categoryDto.getId() == null && categoryDto.getName() == null) {
            throw new BadRequestException("CategoryDto is empty");
        }
        return categoryService.patchCategory(categoryDto, catId);
    }

    @DeleteMapping("/{catId}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteCategory(@PathVariable Long catId) {
        categoryService.deleteCategory(catId);
    }
}