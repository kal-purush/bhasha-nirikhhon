package com.example.ecommerce.model;

import jakarta.persistence.*;

import javax.validation.constraints.NotBlank;

@Entity
@Table(name = "address")
public class Address {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;

    @Column(name = "addressLine1")
    private @NotBlank String addressLine1;

    @Column(name = "addressLine2")
    private String addressLine2;

    @Column(name = "zipCode")
    private @NotBlank int zipCode;

    @Column(name = "city")
    private @NotBlank String city;

    @Column(name = "state")
    private @NotBlank String state;

    @Column(name = "phone")
    private String phone;

    @Column(name = "isPrimary", columnDefinition = "boolean default false")
    private boolean isPrimary;

    @ManyToOne
    @JoinColumn(name = "user_id")
    private User user;
    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getAddressLine2() {
        return addressLine2;
    }

    public void setAddressLine2(String firstName) {
        this.addressLine2 = firstName;
    }

    public String getAddressLine1() {
        return addressLine1;
    }

    public void setAddressLine1(String lastName) {
        this.addressLine1 = lastName;
    }

    public int getZipCode() {
        return zipCode;
    }

    public void setZipCode(int zipCode) {
        this.zipCode = zipCode;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public boolean isPrimary() {
        return isPrimary;
    }

    public void setPrimary(boolean primary) {
        isPrimary = primary;
    }

    public User getUser() {
        return user;
    }

    public void setUser(User user) {
        this.user = user;
    }


}