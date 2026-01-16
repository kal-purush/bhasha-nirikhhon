package com.application.hirebase.user;

import javax.persistence.*;

@Entity
@Table(name = "users")
public class User {
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "user_id")
    private int userID;

    @Column(name = "user_email_id")
    private String userEmailID;

    @Column(name = "user_role")
    private String userRole;

    @Column(name = "is_active")
    private boolean isUserActive; //all the users are by default active when registered or added

    public User() {
    }

    public User(int userID, String userEmailID, String userRole, boolean isUserActive) {
        this.userID = userID;
        this.userEmailID = userEmailID;
        this.userRole = userRole;
        this.isUserActive = isUserActive;
    }

    public int getUserID() {
        return userID;
    }

    public void setUserID(int userID) {
        this.userID = userID;
    }

    public String getUserEmailID() {
        return userEmailID;
    }

    public void setUserEmailID(String userEmailID) {
        this.userEmailID = userEmailID;
    }

    public String getUserRole() {
        return userRole;
    }

    public void setUserRole(String userRole) {
        this.userRole = userRole;
    }

    public boolean isUserActive() {
        return isUserActive;
    }

    public void setUserActive(boolean userActive) {
        isUserActive = userActive;
    }
}