package ru.practicum.ewm.category.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import ru.practicum.ewm.category.model.Category;
import ru.practicum.ewm.category.service.CategoryServiceAdmin;

import javax.validation.Valid;
import javax.validation.constraints.Positive;

@RestController
@RequiredArgsConstructor
@Slf4j
@Validated
@RequestMapping(path = "/admin/categories")
public class CategoryControllerAdmin {

    private final CategoryServiceAdmin categoryServiceAdmin;

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Category addCategory(@RequestBody @Valid Category newCategory) {
        log.info("Принят запрос на добавление новой категории");
        return categoryServiceAdmin.addCategory(newCategory);
    }

    @DeleteMapping("/{catId}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteUser(@PathVariable @Positive Integer catId) {
        log.info("Принят запрос на удаление категории ID: {}", catId);
        categoryServiceAdmin.deleteCategory(catId);
    }


    @PatchMapping("/{catId}")
    @ResponseStatus(HttpStatus.OK)
    public Category patchCategory(@PathVariable @Positive Integer catId,
                                  @RequestBody @Valid Category patchedCategory) {
        log.info("Принят запрос на изменение категории ID: {}", catId);
        return categoryServiceAdmin.patchCategory(catId, patchedCategory);
    }
}