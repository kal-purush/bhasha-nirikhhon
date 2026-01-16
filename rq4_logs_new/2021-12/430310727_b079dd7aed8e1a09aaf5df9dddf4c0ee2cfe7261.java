package ru.otus.spring.service.impl;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import ru.otus.spring.domain.Genre;
import ru.otus.spring.repositories.GenreRepository;
import ru.otus.spring.service.GenreService;

import java.util.List;

@RequiredArgsConstructor
@Service
public class GenreAuthorService implements GenreService {

    private final GenreRepository dao;

    @Override
    public List<Genre> getGenres() {
        return dao.getGenres();
    }
}