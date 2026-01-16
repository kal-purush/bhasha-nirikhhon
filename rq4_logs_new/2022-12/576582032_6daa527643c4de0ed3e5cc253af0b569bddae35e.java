package ru.yandex.practicum.filmorate.controllers;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public abstract class AbstractController <T> {
    HashMap <Integer, T> elements = new HashMap<>();
    Integer elementID = 0;

    @GetMapping
    public List<T> getAll(){
        List<T> elementList =new ArrayList<>(elements.values());
        return elementList;
    }

    @PostMapping
    public T create(@Valid @RequestBody T element) {
        elements.put(++elementID, element);
        return element;
    };

    @PutMapping
    public abstract T update(@Valid @RequestBody T element);
}