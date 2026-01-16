package ch.volleymanager.domain;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

public class AbstractPerson {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    @Column(nullable = false)
    private String firstName;
    @Column(nullable = false)
    private String lastName;
    @Column(nullable = false)
    private String street;
    @Column(nullable = false, updatable = true)
    private int streetNb;
    @Column(nullable = false, updatable = true)
    private int postalCode;
    @Column(nullable = false, updatable = true)
    private String location;

    public AbstractPerson(Long id, String firstName, String lastName, String street, int streetNb, int postalCode, String location) {
        this.id = id;
        this.firstName = firstName;
        this.lastName = lastName;
        this.street = street;
        this.streetNb = streetNb;
        this.postalCode = postalCode;
        this.location = location;
    }

    public AbstractPerson(){
        }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public String getStreet() {
        return street;
    }

    public void setStreet(String street) {
        this.street = street;
    }

    public int getStreetNb() {
        return streetNb;
    }

    public void setStreetNb(int streetNb) {
        this.streetNb = streetNb;
    }

    public int getPostalCode() {
        return postalCode;
    }

    public void setPostalCode(int postalCode) {
        this.postalCode = postalCode;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }
}