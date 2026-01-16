package com.example.demo.model;

import lombok.Getter;
import lombok.Setter;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

@Getter
@Setter
@Entity
public class Portfolio {

    private String image;
    private String title;
    private String content;

    public Portfolio() {
    }

    public Portfolio(String image) {
        this.image = image;
    }

    @Id
    @GeneratedValue
    private long idx;

}