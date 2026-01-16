package com.glii.ddbackmanage.vo;

import lombok.Data;
@Data
public class ResultTreeVO {
    /**
     * 状态信息
     * */
    private Status status = new Status();

    /**
     * 返回数据
     * */
    private Object data;

    /**
     * 所需内部类
     * */
    @Data
    public class Status{

        private Integer code = 200;

        private String message = "成功";
    }
}