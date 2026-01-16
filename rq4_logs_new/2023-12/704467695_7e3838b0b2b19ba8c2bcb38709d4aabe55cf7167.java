package ru.yandex.practicum.filmorate.Controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.filmorate.model.Directors;
import ru.yandex.practicum.filmorate.service.DirectorService;

import javax.validation.Valid;
import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("/directors")
public class DirectorsController {

    private final DirectorService directorService;

    @Autowired
    public DirectorsController(DirectorService directorService) {
        this.directorService = directorService;
    }

    @GetMapping
    public List<Directors> getAllDirectors() {
        return directorService.getAllDirectors();
    }

    @GetMapping("/{id}")
    public Directors getDirectorByID(@PathVariable("id") Long id) {
        return directorService.getDirectorByID(id);
    }

    @PutMapping
    public Directors updateDirector(@Valid @RequestBody Directors director) {
        return directorService.updateDirector(director);
    }

    @PostMapping
    public Directors createDirector(@Valid @RequestBody Directors director) {
        return directorService.createDirector(director);
    }

    @DeleteMapping("/{id}")
    public void deleteDirector(@PathVariable("id") Long id) {
        directorService.deleteDirector(id);
    }
}