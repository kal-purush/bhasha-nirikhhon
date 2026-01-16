package com.epam.mysite.domain.entity.content;

import com.google.gson.annotations.SerializedName;

public class EmployeeEntity {
    private int id;
    @SerializedName("fullname")
    private String fullName;

    public EmployeeEntity() {
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getFullName() {
        return fullName;
    }

    public void setFullName(String fullName) {
        this.fullName = fullName;
    }
}