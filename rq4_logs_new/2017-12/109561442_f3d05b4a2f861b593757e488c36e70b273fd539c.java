package com.gandazhi.sell.common;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.io.Serializable;

/**
 * 上传图片返回数据专用
 * 成功必须返回  success：1
 * 失败直接返回  错误原因
 * @author gandazhi
 * @create 2017-11-30 下午10:30
 **/
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class UploadServiceResponse implements Serializable{

    private int success;
    private String msg;

    public UploadServiceResponse(int success, String url) {
        this.success = success;
        this.msg = url;
    }

    public UploadServiceResponse(String msg) {
        this.msg = msg;
    }

    public int getSuccess() {
        return success;
    }

    public String getUrl() {
        return msg;
    }

    public static UploadServiceResponse createBySuccess(String msg) {
        return new UploadServiceResponse(1, msg);
    }

    public static UploadServiceResponse createByError(String msg){
        return new UploadServiceResponse(msg);
    }
}