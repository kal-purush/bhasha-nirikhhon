package com.stl.db;

import org.springframework.security.core.userdetails.User;

public interface UserDB {

    void save(User user);
}