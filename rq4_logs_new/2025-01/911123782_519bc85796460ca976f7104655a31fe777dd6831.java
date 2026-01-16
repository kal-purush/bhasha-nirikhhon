    package com.roomfit.be.user;

import com.roomfit.be.global.entity.BaseEntity;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity(name="users")
@Getter
@NoArgsConstructor
public class User extends BaseEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private String nickname;
    private String email;
    private String password;
    @Enumerated(EnumType.STRING)
    private UserRole role;

    public User(String nickname, String email, String password, UserRole role) {
        this.nickname = nickname;
        this.email = email;
        this.password = password;
        this.role = role;
    }

    public static User createAdmin(String nickname, String email, String password){
        return new User(nickname, email, password, UserRole.ADMIN);
    }
    public static User createUser(String nickname, String email, String password){
        return new User(nickname, email, password, UserRole.DEFAULT);
    }
}