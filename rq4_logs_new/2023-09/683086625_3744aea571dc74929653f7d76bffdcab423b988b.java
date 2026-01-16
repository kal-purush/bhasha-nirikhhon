package com.singo.Account.user;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;


@Repository
public class UserDataAccess {
    private final JdbcTemplate jdbcTemplate;

    @Autowired
    public UserDataAccess(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    //get user
    public User getUser(String email){
        String sql = "SELECT * FROM _user where email = ?";
        return jdbcTemplate.queryForObject(sql,new UserMapper(),new Object[]{email});
    }

}