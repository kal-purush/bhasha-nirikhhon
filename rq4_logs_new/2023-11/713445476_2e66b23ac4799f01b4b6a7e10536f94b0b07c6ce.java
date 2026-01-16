package com.example.haruhanal.dto;

import com.example.haruhanal.entity.User;
import com.example.haruhanal.enums.Gender;
import com.example.haruhanal.enums.UserRole;
import lombok.Builder;
import lombok.Data;

@Data
public class LoginDTO {
    private Long id;
    private String name;
    private Gender gender;
    private String email;
    private int age;
    private String address;
    private String condition;
    private Integer subscribe;
    private UserRole userRole;
    private Integer alreadyMember; //0은 신규 1은 기존

    public LoginDTO(User user) {
        this.id = user.getId();
        this.name = user.getName();
        this.gender = user.getGender();
        this.email = user.getEmail();
        this.age = user.getAge();
        this.address = user.getAddress();
        this.condition = user.getCondition();
        this.subscribe = user.getSubscribe();
        this.userRole = user.getUserRole();
    }

    @Builder
    public LoginDTO(Long id, String name, Gender gender, String email, int age, String address, String condition, Integer subscribe, UserRole userRole, Integer alreadyMember) {
        this.id = id;
        this.name = name;
        this.gender = gender;
        this.email = email;
        this.age = age;
        this.address = address;
        this.condition = condition;
        this.subscribe = subscribe;
        this.userRole = userRole;
        this.alreadyMember = alreadyMember;
    }
}