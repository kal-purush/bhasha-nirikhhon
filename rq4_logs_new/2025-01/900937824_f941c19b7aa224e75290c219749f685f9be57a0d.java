package ru.yandex.practicum.filmorate.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.filmorate.model.Genre;
import ru.yandex.practicum.filmorate.service.GenreService;

import java.util.List;

@RestController
@Slf4j
@RequestMapping("/genres")
@RequiredArgsConstructor
public class GenreController {
    private final GenreService genreService;

    @GetMapping
    @ResponseStatus(HttpStatus.OK)
    public List<Genre> getAllGenres() {
        log.info("получение всех жанров");
        return genreService.getAllGenres();
    }

    @GetMapping("/{id}")
    @ResponseStatus(HttpStatus.OK)
    public Genre getGenreById(@PathVariable int id) {
        log.info("получение жанра по ид={}", id);
        return genreService.getGenreById(id);
    }

//    @PostMapping("/films")
//    public void setFilmGenres(@RequestBody List<Film> films) {
//        genreService.setFilmGenres(films);
//    }
//
//    @GetMapping("/films")
//    public List<Genre> loadFilmGenres(@RequestBody List<Film> films) {
//        return genreService.loadFilmGenres(films);
//    }
//
//    @GetMapping("/films/ids")
//    public List<Genre> getGenresByFilmIds(@RequestParam List<Long> filmIds) {
//        return genreService.getGenresByFilmIds(filmIds);
//    }
//
//    @GetMapping("/ids")
//    public List<Genre> getGenresByIds(@RequestParam List<Long> genreIds) {
//        return genreService.getGenresByIds(genreIds);
//    }


}

