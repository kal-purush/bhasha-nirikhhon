package com.eidiko.supermarket_action_service.dao;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class IncentiveRepo {

    private final JdbcTemplate jdbcTemplate;

    @Autowired
    public IncentiveRepo(@Qualifier("postgresqlJdbcTemplate") JdbcTemplate jdbcTemplate)
    {
        this.jdbcTemplate=jdbcTemplate;
    }


}