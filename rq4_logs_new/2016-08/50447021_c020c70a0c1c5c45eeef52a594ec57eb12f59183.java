package com.dk.dento.care.model;

import org.hibernate.validator.constraints.NotBlank;

import javax.validation.constraints.NotNull;


/**
 * Created by khana on 09/08/16.
 */
public class Clinic {

    private Long id;

    //@TODO javax.validations are not working, fix this.
    @NotNull
    private String name;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}