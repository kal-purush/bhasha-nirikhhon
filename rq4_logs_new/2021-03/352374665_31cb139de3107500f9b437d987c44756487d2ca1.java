package com.computerservice.dao.test;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.Collections;
import java.util.List;

import static com.computerservice.dao.test.TestDaoQueries.GET_ALL_NAMES;

@Repository
@RequiredArgsConstructor
public class TestDao {

    private final JdbcTemplate jdbcTemplate;

    public List<String> getTestNames() {
        List<String> strings = jdbcTemplate.query(
                GET_ALL_NAMES,
                new TestMapper()
        );
        if (!strings.isEmpty()) {
            return strings;
        }
        return Collections.emptyList();
    }
}