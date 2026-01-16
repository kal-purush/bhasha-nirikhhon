package com.sxxh.linghuo.issus.bean;

public class IssusBean {
    private int code;
    private String msg;
    private DataBean data;

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public DataBean getData() {
        return data;
    }

    public void setData(DataBean data) {
        this.data = data;
    }

    public static class DataBean {
        private String task_id;

        public String getTask_id() {
            return task_id;
        }

        public void setTask_id(String pTask_id) {
            task_id = pTask_id;
        }
    }
}