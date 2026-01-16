package com.android.quemeful_qr;

public class Attendee {
    private String id;
    private String firstName;
    private String lastName;

    private Integer checkedIn;

    public Attendee(String id, String firstName, String lastName, Integer checkedIn) {
        this.id = id;
        this.firstName = firstName;
        this.lastName = lastName;
        this.checkedIn = checkedIn;
    }

    public int getCheckedIn() {
        return checkedIn;
    }

    public void setCheckedIn(Integer checkedIn) {
        this.checkedIn = checkedIn;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
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

    // getters and setters for all fields
}