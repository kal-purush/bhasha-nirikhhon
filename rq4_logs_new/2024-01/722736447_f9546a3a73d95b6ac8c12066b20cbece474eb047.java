package ru.yandex.practicum.filmorate.dao.impl;

import lombok.RequiredArgsConstructor;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.filmorate.dao.GenresDbStorage;
import ru.yandex.practicum.filmorate.exeption.EntityNotFoundException;
import ru.yandex.practicum.filmorate.model.Genre;

import java.util.List;

@Service
@RequiredArgsConstructor
public class GenresDbStorageImpl implements GenresDbStorage {
    private final JdbcTemplate jdbcTemplate;

    @Override
    public List<Genre> genresFindAll() {
        String sql = "select * from genres order by id";
        return jdbcTemplate.query(sql, (rs, rowNum) -> new Genre(rs.getInt("id"), rs.getString("name")));
    }

    @Override
    public Genre genresFindForId(int id) {
        String sql = "select * from genres where id = ? order by id";
        try {
            return jdbcTemplate.queryForObject(sql, (rs, rowNum) -> new Genre(rs.getInt("id"), rs.getString("name")), id);
        } catch (EmptyResultDataAccessException e) {
            throw new EntityNotFoundException("Нет жанра с id: " + id);
        }
    }
}