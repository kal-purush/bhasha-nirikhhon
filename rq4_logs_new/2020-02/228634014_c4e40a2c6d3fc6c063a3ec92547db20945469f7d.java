package net.tch.java.serializer.entity;

import java.io.Serializable;

/**
 * @description:
 * @auth tongchenghao
 * @date 2020/2/12
 */
public class User implements Serializable {

    private String name;

    private String sex;

    private Hobby hobby;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public Hobby getHobby() {
        return hobby;
    }

    public void setHobby(Hobby hobby) {
        this.hobby = hobby;
    }
}