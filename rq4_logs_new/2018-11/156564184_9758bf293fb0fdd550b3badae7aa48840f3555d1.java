package com.example.rseservice.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import java.time.LocalDate;

@Entity
public class Client {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;
    private String name;
    private String document;
    private LocalDate birthDay;

    public Client() {
    }

    public Client(String name, String document, LocalDate birthDay) {
        this.name = name;
        this.document = document;
        this.birthDay = birthDay;
    }

    public Client(Long id, String name, String document, LocalDate birthDay) {
        this.id = id;
        this.name = name;
        this.document = document;
        this.birthDay = birthDay;
    }

    public Long getId() {
        return id;
    }

    public String getDocument() {
        return document;
    }

    @JsonIgnore
    public boolean isNew() {
        return getId() == null;
    }

    @JsonIgnore
    public boolean alreadyExist() {
        return getId() != null;
    }

    @Override
    public String toString() {
        return "Client{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", document='" + document + '\'' +
                ", birthDay=" + birthDay +
                '}';
    }
}