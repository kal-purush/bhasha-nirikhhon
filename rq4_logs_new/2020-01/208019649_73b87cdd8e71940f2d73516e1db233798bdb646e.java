package com.lh.nailweb.security.auth;

import org.springframework.security.core.GrantedAuthority;

/**
 * @auther: loneyfall
 * @date: 2019/12/26
 * @description:
 */
public class SimpleAuthority implements GrantedAuthority {
    private String auth;

    public SimpleAuthority(String auth) {
        this.auth = auth;
    }

    @Override
    public String getAuthority() {
        return auth;
    }
}