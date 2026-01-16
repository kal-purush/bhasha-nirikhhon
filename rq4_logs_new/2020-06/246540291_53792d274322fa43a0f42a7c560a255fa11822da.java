package org.pesmypetcare.webservice.entity.usermanager;

import lombok.Data;

/**
 * @author Santiago Del Rey
 */
@Data
public class User {
    private String uid;
    private String username;
    private String password;
    private String email;

    public User() { }

    public User(String uid, String username, String password, String email) {
        this.uid = uid;
        this.username = username;
        this.password = password;
        this.email = email;
    }
}