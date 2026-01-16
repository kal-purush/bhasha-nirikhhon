package com.example.blogit.user;

import java.util.UUID;

public class UserDto {
    private Long id;
    private UUID uuid;
    private String email;
    private String username;
    private String profilePicture;

    public UserDto(Long id, UUID uuid, String email, String username, String profilePicture) {
        this.id = id;
        this.uuid = uuid;
        this.email = email;
        this.username = username;
        this.profilePicture = profilePicture;
    }

    public Long getId() {
        return id;
    }

    public UUID getUuid() {
        return uuid;
    }

    public String getEmail() {
        return email;
    }

    public String getUsername() {
        return username;
    }

    public String getProfilePicture() {
        return profilePicture;
    }
}