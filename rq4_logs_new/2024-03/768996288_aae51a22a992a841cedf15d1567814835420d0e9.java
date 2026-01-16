package api.auth.dto;

import api.user.User;

import java.time.LocalDateTime;

public class SignUpDto {
    private String email;
    private User.UserType userType;
    private String pw;
    private String contact;
    private User.Gender gender;
    private LocalDateTime birthday;

    public SignUpDto(String email, User.UserType userType, String pw, String avatarUrl, String contact, User.Gender gender, LocalDateTime birthday){
        this.email = email;
        this.userType = userType;
        this.pw = pw;
        this.contact = contact;
        this.gender = gender;
        this.birthday = birthday;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public User.UserType getUserType() {
        return userType;
    }

    public void setUserType(User.UserType userType) {
        this.userType = userType;
    }

    public String getPw() {
        return pw;
    }

    public void setPw(String pw) {
        this.pw = pw;
    }

    public String getContact() {
        return contact;
    }

    public void setContact(String contact) {
        this.contact = contact;
    }

    public User.Gender getGender() {
        return gender;
    }

    public void setGender(User.Gender gender) {
        this.gender = gender;
    }

    public LocalDateTime getBirthday() {
        return birthday;
    }

    public void setBirthday(LocalDateTime birthday) {
        this.birthday = birthday;
    }
}