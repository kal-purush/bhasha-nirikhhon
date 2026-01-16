package com.huiguan.demo.api.entity;

import java.io.Serializable;

/**
 * Created by ansoya on 2017/4/28.
 */
public class User implements Serializable {
    private String id;

    public String getId(){
        return id;
    }
    public void setId(String id){
        this.id=id;
    }
}