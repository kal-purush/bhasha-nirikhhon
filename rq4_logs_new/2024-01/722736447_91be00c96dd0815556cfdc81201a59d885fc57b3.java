package ru.yandex.practicum.filmorate.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.filmorate.dao.GenresStorage;
import ru.yandex.practicum.filmorate.model.Genre;

import java.util.List;

@Service
@RequiredArgsConstructor
public class GenresService {
    private final GenresStorage genresStorage;

    public List<Genre> genresFindAll() {
        return genresStorage.genresFindAll();
    }

    public Genre genresFindForId(int id) {
        return genresStorage.genresFindForId(id);
    }
}