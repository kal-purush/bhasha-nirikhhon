package com.pretz.dyntreeserver.service;

import org.springframework.http.HttpHeaders;

public class JsonApiHeaders extends HttpHeaders {

    public JsonApiHeaders() {
        super();
        this.add(CONTENT_TYPE, "application/vnd.api+json");
    }
}