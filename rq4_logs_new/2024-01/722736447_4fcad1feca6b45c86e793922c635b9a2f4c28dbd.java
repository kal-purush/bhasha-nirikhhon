package ru.yandex.practicum.filmorate.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.filmorate.dao.MpaDbStorage;
import ru.yandex.practicum.filmorate.dao.impl.MpaDbStorageImpl;
import ru.yandex.practicum.filmorate.model.Mpa;

import java.util.List;

@Slf4j
@RestController
@RequestMapping("/mpa")
@RequiredArgsConstructor
public class MpaController {
    private final MpaDbStorageImpl mpaDbStorage;

    @GetMapping()
    public List<Mpa> mpaFindAll(){
        return mpaDbStorage.mpaFindAll();
    }
    @GetMapping("/{id}")
    public Mpa mpaFindAll(@PathVariable int id){
        return mpaDbStorage.findMpaForId(id);
    }
}