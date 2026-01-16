package ru.yandex.practicum.filmorate.integral_tests;

import lombok.RequiredArgsConstructor;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.jdbc.JdbcTest;
import org.springframework.context.annotation.Import;
import ru.yandex.practicum.filmorate.model.Genre;
import ru.yandex.practicum.filmorate.repository.impl.JdbcGenreRepository;
import ru.yandex.practicum.filmorate.repository.mappers.GenreRowMapper;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@JdbcTest
@AutoConfigureTestDatabase
@RequiredArgsConstructor(onConstructor_ = @Autowired)
@Import({JdbcGenreRepository.class, GenreRowMapper.class})
public class GenreDBRepositoryTest {
    private final JdbcGenreRepository genreRepository;


    @Test
    public void testGetById() {
        Optional<Genre> genre = genreRepository.getById(5);
        assertThat(genre).isPresent().hasValueSatisfying(genreIn -> assertThat(genreIn).hasFieldOrPropertyWithValue("id", 5));

    }

    @Test
    public void testGetAll() {
        List<Genre> all = genreRepository.getAll();
        assertThat(all).hasSize(6);

        assertThat(all).extracting(Genre::getName).containsExactlyInAnyOrder("Комедия", "Драма", "Боевик", "Мультфильм", "Триллер", "Документальный");

        assertThat(all).extracting(Genre::getId).containsExactlyInAnyOrder(1, 2, 3, 4, 5, 6);
    }

}