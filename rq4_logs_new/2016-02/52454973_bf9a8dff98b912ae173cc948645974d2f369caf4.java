package com.example.lenovo.myapp;

import java.io.Serializable;

/**
 * Created by lenovo on 2016/2/24.
 */
public class UserInfo implements Serializable {
    private String mUserName;
    private int mAge;

    public UserInfo(String mUserName, int mAge ) {
        this.mAge = mAge;
        this.mUserName = mUserName;
    }

    public String getmUserName() {
        return mUserName;
    }

    public void setmUserName(String mUserName) {
        this.mUserName = mUserName;
    }

    public int getmAge() {
        return mAge;
    }

    public void setmAge(int mAge) {
        this.mAge = mAge;
    }
}