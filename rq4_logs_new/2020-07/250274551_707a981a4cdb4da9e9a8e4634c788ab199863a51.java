package com.zubko.homework.hw_24;

import java.util.Objects;

public class NetworkUser {
    private String id;
    private String email;
    private String password;

    public NetworkUser(String id, String email, String password) {
        this.id = id;
        this.email = email;
        this.password = password;
    }

    public NetworkUser() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    @Override
    public String toString() {
        return "NetworkUser{" +
                "id='" + id + '\'' +
                ", email='" + email + '\'' +
                ", password='" + password + '\'' +
                '}';
    }

    public String toJson() {
        return "{" +
                "\"id\"=\"" + id + '\"' +
                ", \"email\"=\"" + email + '\"' +
                ", \"password\"=\"" + password + '\"' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NetworkUser that = (NetworkUser) o;
        return Objects.equals(id, that.id) &&
                Objects.equals(email, that.email) &&
                Objects.equals(password, that.password);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, email, password);
    }
}