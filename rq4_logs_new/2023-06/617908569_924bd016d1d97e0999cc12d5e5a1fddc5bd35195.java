package ru.yandex.practicum.filmorate.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.filmorate.model.Director;
import ru.yandex.practicum.filmorate.service.DirectorService;

import javax.validation.Valid;
import java.util.List;

@RestController
@RequestMapping("/directors")
@RequiredArgsConstructor
@Slf4j
public class DirectorController {
    private final DirectorService directorService;

    @PostMapping
    public Director add(@Valid @RequestBody Director director) {
        log.info("поступил запрос на добавление режиссера с name=" + director.getName());
        return directorService.add(director);
    }

    @PutMapping
    public Director update(@Valid @RequestBody Director director) {
        log.info("поступил запрос на обновление режиссера с id " + director.getId());
        return directorService.update(director);
    }

    @DeleteMapping("/{id}")
    public void delete(@PathVariable int id) {
        log.info("поступил запрос на удаление режиссера с id " + id);
        directorService.delete(id);
    }

    @GetMapping
    public List<Director> getAllDirectors() {
        log.info("поступил запрос получение всех режиссеров");
        return directorService.getAllDirectors();
    }

    @GetMapping("/{id}")
    public Director getById(@PathVariable int id) {
        log.info("поступил запрос получения режиссера с id " + id);
        return directorService.getById(id);
    }
}