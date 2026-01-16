package com.codeup.mentor.model;

import javax.persistence.*;

@Entity
@Table(name="users")
public class User {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(nullable = false)
    private long id;

    @Column(nullable = false)
    private String first_name;

    @Column(nullable = false)
    private String last_name;

    @Column(nullable = false, unique = true)
    private String username;

    @Column(nullable = false)
    private String email;

    @Column(nullable = false, length=500)
    private String biography;

    @Column(nullable = false)
    private boolean is_mentor;

    @Column
    private String filestack_picture_url;

    public User (){};

    public User(long id, String first_name, String last_name, String username, String email, String biography, boolean is_mentor, String filestack_picture_url) {
        this.id = id;
        this.first_name = first_name;
        this.last_name = last_name;
        this.username = username;
        this.email = email;
        this.biography = biography;
        this.is_mentor = is_mentor;
        this.filestack_picture_url = filestack_picture_url;
    }

    public User(String first_name, String last_name, String username, String email, String biography, boolean is_mentor, String filestack_picture_url) {
        this.first_name = first_name;
        this.last_name = last_name;
        this.username = username;
        this.email = email;
        this.biography = biography;
        this.is_mentor = is_mentor;
        this.filestack_picture_url = filestack_picture_url;
    }

    public User(User copy){
        id = copy.id;
        first_name = copy.first_name;
        last_name = copy.last_name;
        username = copy.username;
        email = copy.email;
        biography = copy.biography;
        is_mentor = copy.is_mentor;
        filestack_picture_url = copy.filestack_picture_url
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getFirst_name() {
        return first_name;
    }

    public void setFirst_name(String first_name) {
        this.first_name = first_name;
    }

    public String getLast_name() {
        return last_name;
    }

    public void setLast_name(String last_name) {
        this.last_name = last_name;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getBiography() {
        return biography;
    }

    public void setBiography(String biography) {
        this.biography = biography;
    }

    public boolean isIs_mentor() {
        return is_mentor;
    }

    public void setIs_mentor(boolean is_mentor) {
        this.is_mentor = is_mentor;
    }

    public String getFilestack_picture_url() {
        return filestack_picture_url;
    }

    public void setFilestack_picture_url(String filestack_picture_url) {
        this.filestack_picture_url = filestack_picture_url;
    }


}