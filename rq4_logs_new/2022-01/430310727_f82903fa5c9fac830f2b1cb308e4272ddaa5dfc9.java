package ru.otus.spring.service.impl;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ru.otus.spring.domain.Genre;
import ru.otus.spring.repositories.GenreRepository;
import ru.otus.spring.service.GenreService;

import java.util.List;

@Component
@RequiredArgsConstructor
public class SimpleGenreService implements GenreService {

    private final GenreRepository genreRepository;
    static final String NULL_GENRE = "Didn't find genre";


    @Override
    public List<Genre> getGenres() {
        return genreRepository.getGenres();
    }

    @Override
    public Genre getGenreById(long id) {
        Genre genreById = genreRepository.getGenreById(id).orElse(null);
        if (genreById != null) {
            return genreById;
        }
        throw new RuntimeException(NULL_GENRE);
    }

    @Override
    public long create(String name) {
        Genre genre = new Genre(0, name);
        return genreRepository.save(genre).getId();
    }

    @Override
    public void delete(long id) {
        Genre genre = genreRepository.getGenreById(id).orElse(null);
        if (genre != null) {
            genreRepository.delete(genre);
        } else throw new RuntimeException(NULL_GENRE);
    }
}