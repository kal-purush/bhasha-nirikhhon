package org.spiceboys.Travel.Diary.dto;

import jakarta.validation.constraints.Email;

public class AuthorisedUserDTO extends PublicUserDTO{
    private String firstName;
    private String lastName;
    @Email
    private String email;

    public AuthorisedUserDTO(Long userId,
                         String username,
                         String bio,
                         String profilePicUrl,
                         Boolean isPrivate,
                         String firstName,
                         String lastName,
                         String email) {
        super(userId, username, bio, profilePicUrl, isPrivate);
        this.firstName = firstName;
        this.lastName = lastName;
        this.email = email;
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

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }
}