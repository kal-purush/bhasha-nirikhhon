package com.example.scoreservice.model;

import io.swagger.annotations.ApiModel;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.util.Date;

@Data
@Getter
@Setter
@ApiModel(description = "用户类")
public class User {
    private Integer PID;

    private String SID;

    private String name;

//    private String password;

    private Integer gender;

    private Integer type;

    private Integer nation;

    private String identity;

    private Date birthday;

    private Integer level;

    private String grade;

    private String user_class;

    private Integer outlook;

    private String dorm;

    private String user_native;

    private String tel;

    private String wechat;

    private String email;

    private String address;

    private String emergency_name;

    private String emergency_tel;

    private Integer status;

//    private Integer morality_count;
//    private Integer volunteer_count;
//    private Integer socialwork_count;
//    private Integer competition_count;
//    private Integer paper_count;
//    private Integer patent_count;
//    private Integer copyright_count;
//    private Integer publication_count;
//    private Integer exchange_count;

    private Integer personal;

    private Integer overall;
}