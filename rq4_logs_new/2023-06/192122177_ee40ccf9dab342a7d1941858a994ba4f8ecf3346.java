package org.frank.java.jackson.beans;

import java.util.ArrayList;
import java.util.List;

public class StatusBean {
    private String msg;
    private List<User> data = new ArrayList<>();
    private Integer status;

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public List<User> getData() {
        return data;
    }

    public void setData(List<User> data) {
        this.data = data;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "StatusBean{" +
                "msg='" + msg + '\'' +
                ", data=" + data +
                ", status=" + status +
                '}';
    }
}