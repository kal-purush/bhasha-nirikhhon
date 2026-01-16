package com.infonal.model.response;

import com.infonal.entity.User;
import com.infonal.util.Response;

import java.util.List;

/**
 * Created by hikuley on 22.08.2015.
 */
public class UserFindAllResponse extends Response {

    private List<User> userList;

    public List<User> getUserList() {
        return userList;
    }

    public void setUserList(List<User> userList) {
        this.userList = userList;
    }
}