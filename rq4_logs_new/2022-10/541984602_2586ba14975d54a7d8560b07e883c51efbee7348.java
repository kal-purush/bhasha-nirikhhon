package com.kainos.ea.models;

import org.checkerframework.checker.signature.qual.Identifier;

import javax.validation.constraints.NotEmpty;

public class Band {

    @NotEmpty
    @Identifier
    private int id;
    @NotEmpty
    private String name;

    public Band() {
    }

    public Band(int id, String name) {
        this.id = id;
        this.name = name;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}