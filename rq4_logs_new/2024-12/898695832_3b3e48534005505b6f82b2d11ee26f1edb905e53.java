package ru.clevertec.carshowroom.controller;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import ru.clevertec.carshowroom.dto.category.CategoryRequest;
import ru.clevertec.carshowroom.dto.category.CategoryResponse;
import ru.clevertec.carshowroom.dto.category.UpdateCategoryRequest;
import ru.clevertec.carshowroom.service.CategoryService;

import java.util.List;

@RestController
@Validated
@RequiredArgsConstructor
@RequestMapping("/categories")
public class CategoryController {
    private final CategoryService categoryService;

    @GetMapping
    @ResponseStatus(HttpStatus.OK)
    public List<CategoryResponse> getAllCategories() {
        return categoryService.findAll();
    }

    @GetMapping("/{id}")
    @ResponseStatus(HttpStatus.OK)
    public CategoryResponse getCategoryById(@PathVariable("id") Long id) { //@NotBlank
        return categoryService.findById(id);
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public CategoryRequest addCategory(@RequestBody @Valid CategoryRequest categoryRequest) {
        return categoryService.addCategory(categoryRequest);
    }

    @PutMapping()
    @ResponseStatus(HttpStatus.CREATED)
    public UpdateCategoryRequest update(@RequestBody @Valid UpdateCategoryRequest updateCategoryRequest) {
        return categoryService.update(updateCategoryRequest);
    }

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteById(@PathVariable("id") @Valid @NotBlank Long id) {
        categoryService.deleteById(id);
    }
}