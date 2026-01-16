package com.example.hzxr.tellme.net.model;

import org.simpleframework.xml.Element;
import org.simpleframework.xml.Root;

/**
 * Created by Hzxr on 2018/2/16.
 */
@Root(name = "user", strict = false)
public class User {
    @Element(name = "username")
    public String username;

    public User() {}

    public User(String username) {
        this.username = username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getUsername() {
        return username;
    }
}