package ru.otus.core.model;

import javax.persistence.*;

@Entity
@Table(name = "addresses")
public class Address {

    @Id
    @Column(name = "id_user")
    private Long id;

    @OneToOne
    @MapsId
    @JoinColumn(name = "id_user")
    private User user;

    @Column(name = "street")
    private String street;

    public Address() {
    }

    public Address(User user, String street) {
        this.id = user.getId();
        this.user = user;
        this.street = street;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public User getUser() {
        return user;
    }

    public void setUser(User user) {
        this.user = user;
    }

    public String getStreet() {
        return street;
    }

    public void setStreet(String street) {
        this.street = street;
    }
}