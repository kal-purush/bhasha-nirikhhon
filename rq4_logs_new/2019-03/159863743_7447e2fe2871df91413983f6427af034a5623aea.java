package com.university.contractors.config;

import org.springframework.security.core.AuthenticationException;

class BadAuthenticationRequest extends AuthenticationException {

    BadAuthenticationRequest(String msg, Throwable t) {
        super(msg, t);
    }
}