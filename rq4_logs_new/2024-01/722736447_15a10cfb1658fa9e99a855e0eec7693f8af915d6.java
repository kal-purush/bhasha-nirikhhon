package ru.yandex.practicum.filmorate.dao.impl;

import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.filmorate.dao.UserDbStorage;
import ru.yandex.practicum.filmorate.exeption.EntityNotFoundException;
import ru.yandex.practicum.filmorate.model.User;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

@Service
public class UserDbStorageImpl implements UserDbStorage {
    private final JdbcTemplate jdbcTemplate;

    public UserDbStorageImpl(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public List<User> findAll() {
        String sql = "select * from users";
        return jdbcTemplate.query(sql, userRowMapper());
    }

    @Override
    public User findUserById(int id) {
        String sql = "select * from users where id = ?";
        User findUser;
        try {
            findUser = jdbcTemplate.queryForObject(sql, userRowMapper(), id);
        } catch (EmptyResultDataAccessException e) {
            findUser = null;
        }
        if (findUser == null) {
            throw new EntityNotFoundException("Нет пользователя с ID: " + id);
        } else {
            return findUser;
        }
    }

    @Override
    public User post(User user) {
        SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(jdbcTemplate.getDataSource()).withTableName("users").usingGeneratedKeyColumns("id");
        Map<String, String> params = Map.of("email", user.getEmail(), "login", user.getLogin(), "name", user.getName(), "birthday", user.getBirthday().toString());
        Number id = simpleJdbcInsert.executeAndReturnKey(params);
        user.setId(id.intValue());
        return user;


    }

    @Override
    public User put(User user) {
        String sqlFindUser = "select * from users where id = ?";
        String sqlUpdate = "update users set login = ?,name = ?, email=?,birthday =? where id = ?";
        User findUser;
        try {
            findUser = jdbcTemplate.queryForObject(sqlFindUser, userRowMapper(), user.getId());
        } catch (EmptyResultDataAccessException e) {
            findUser = null;
        }
        if (findUser == null) {
            throw new EntityNotFoundException("Нет пользователя с ID: " + user.getId());
        } else {
            jdbcTemplate.update(sqlUpdate, user.getLogin(), user.getName(), user.getEmail(), user.getBirthday(), user.getId());
            return user;
        }
    }

    private RowMapper<User> userRowMapper() {
        return (rs, rowNum) -> new User(rs.getInt("id"), rs.getString("email"), rs.getString("login"), rs.getString("name"), LocalDate.parse(rs.getString("birthday")));
    }
}