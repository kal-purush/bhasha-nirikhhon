package ru.yandex.practicum.filmorate.dao.implement;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.filmorate.dao.FeedDao;

import java.util.List;

@Repository
@Slf4j
public class FeedDaoImpl implements FeedDao {
    private final JdbcTemplate jdbcTemplate;

    @Autowired
    public FeedDaoImpl(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void addFeed() {

    }

    @Override
    public List getFeed(int id) {
        return null;
    }
}