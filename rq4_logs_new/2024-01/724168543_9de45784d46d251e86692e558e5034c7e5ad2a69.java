/*
 * Copyright 2023 Kapeta Inc.
 * SPDX-License-Identifier: MIT
 */
package com.kapeta.spring.security;

public class JWKSException extends RuntimeException {

    public JWKSException(String message) {
        super(message);
    }

    public JWKSException(String message, Throwable cause) {
        super(message, cause);
    }
}