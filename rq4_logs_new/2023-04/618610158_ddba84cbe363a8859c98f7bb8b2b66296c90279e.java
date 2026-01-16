package ru.practicum.ewm.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import ru.practicum.ewm.entity.dto.CategoryDto;
import ru.practicum.ewm.entity.dto.CompilationDto;
import ru.practicum.ewm.service.CategoryService;
import ru.practicum.ewm.service.CompilationService;

import java.util.List;

@Slf4j
@RestController
@RequestMapping(path = "/")
@Validated
public class PublicController {

    private final CategoryService categoryService;
    private final CompilationService compilationService;

    public PublicController(CategoryService categoryService, CompilationService compilationService) {
        this.categoryService = categoryService;
        this.compilationService = compilationService;
    }

    @GetMapping("/categories")
    public List<CategoryDto> getCategories(
            @RequestParam(defaultValue = "0") Integer from,
            @RequestParam(defaultValue = "10") Integer size
    ) {
        return categoryService.getCategories(from, size);
    }

    @GetMapping("/compilations")
    public List<CompilationDto> getCompilations(
            @RequestParam(required = false) Boolean pinned,
            @RequestParam(defaultValue = "0") Integer from,
            @RequestParam(defaultValue = "10") Integer size
    ) {
        return compilationService.getCompilationsPublic(pinned, from, size);
    }




}