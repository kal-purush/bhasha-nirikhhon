package com.study.message.role.req;

import javax.validation.constraints.NotNull;

/**
 * 添加资源请求类
 */
public class AddResourceReq {

    @NotNull
    private String name;

    @NotNull
    private String desc;

    @NotNull
    private Integer type;

    private String parentResourceName;

    @NotNull
    private String role;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
    }

    public String getParentResourceName() {
        return parentResourceName;
    }

    public void setParentResourceName(String parentResourceName) {
        this.parentResourceName = parentResourceName;
    }

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }
}