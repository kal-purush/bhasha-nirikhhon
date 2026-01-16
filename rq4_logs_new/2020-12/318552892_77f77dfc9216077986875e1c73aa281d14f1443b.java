package com.zhang.libweb.enums;

public enum UserTypeEnum {

    /**
     * 学生枚举值
     */
    STUDENT(1,"学生"),
    /**
     * 教师枚举值
     */
    TEACHER (2,"老师"),
    /**
     * 图书管理员枚举值
     */
    LIB_ADMIN(3,"图书管理员"),
    /**
     * 系统管理员枚举值
     */
    SYS_ADMIN(4,"系统管理员");

    private int numl;

    private String name;

    UserTypeEnum(int numl, String name) {
        this.numl = numl;
        this.name = name;
    }

    public int getNuml() {
        return numl;
    }

    public void setNuml(int numl) {
        this.numl = numl;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}