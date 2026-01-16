package ru.practicum.ewm.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import ru.practicum.ewm.entity.dto.CategoryDto;
import ru.practicum.ewm.entity.dto.NewCategoryDto;
import ru.practicum.ewm.entity.dto.UserDto;
import ru.practicum.ewm.entity.dto.UserRequestDto;
import ru.practicum.ewm.service.CategoryService;
import ru.practicum.ewm.service.UserService;

import javax.validation.Valid;
import java.util.List;

@Slf4j
@RestController
@RequestMapping(path = "/admin")
@Validated
public class AdminController {

    private final UserService userService;
    private final CategoryService categoryService;

    @Autowired
    public AdminController(UserService userService, CategoryService categoryService) {
        this.userService = userService;
        this.categoryService = categoryService;
    }

    @GetMapping("/users")
    public List<UserDto> getUsers(
            @RequestParam(required = false) List<Long> ids,
            @RequestParam(defaultValue = "0") Integer from,
            @RequestParam(defaultValue = "10") Integer size
    ) {
        if (ids == null) {
            return userService.getUsersPageable(from, size);
        } else {
            return userService.getUsersByIds(ids);
        }
    }

    @PostMapping("/users")
    public UserDto createUser(@Valid @RequestBody UserRequestDto userRequestDto) {
        return userService.createUser(userRequestDto);
    }

    @PostMapping("/categories")
    public CategoryDto createCategory(@Valid @RequestBody NewCategoryDto newCategoryDto) {
        return categoryService.createCategory(newCategoryDto);
    }
}