package com.practice.studentenrollment.InstitutionAPI;

import jakarta.persistence.*;

@Entity
@Table(name = "institutions")

public class Institution {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private String name;
    private String address;
    private String phone;

    public Institution() {
    }

    public Institution(String name, String address, String phone) {
        this.name = name;
        this.address = address;
        this.phone = phone;
    }

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

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }
    public static Institution institutionNotFound() {
        Institution notFound = new Institution();
        notFound.setName("Institution not found");
        return notFound;
    }


}