package com.java.codeFront.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.persistence.*;
import org.jetbrains.annotations.NotNull;

@Entity
@Table(name = "Users")
public class Users {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int userID;

    @JsonProperty("user_name") // Mapeo del JSON al atributo Java
    @Column(name = "user_name", nullable = false, unique = true)
    private String userName;

    @JsonProperty("user_password") // Mapeo del JSON al atributo Java
    @NotNull
    @Column(name = "user_password", nullable = false)
    private String userPassword;


    @Column(name = "bookmarks")
    private String bookmarks;

    // Getters y Setters
    public int getUserID() {
        return userID;
    }

    public void setUserID(int userID) {
        this.userID = userID;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getUserPassword() {
        return userPassword;
    }

    public void setUserPassword(String userPassword) {
        this.userPassword = userPassword;
    }

    public String getBookmarks() {
        return bookmarks;
    }

    public void setBookmarks(String bookmarks) {
        this.bookmarks = bookmarks;
    }
}