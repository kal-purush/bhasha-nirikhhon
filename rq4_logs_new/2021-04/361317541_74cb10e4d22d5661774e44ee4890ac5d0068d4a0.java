package com.cloud.entities;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 返回前端的json字符串
 * @param <T>
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class CommonResult<T> {
    private  Integer code;
    private  String message;
    private T data;

    public  CommonResult(Integer code,String message){
        this(code,message,null);
    }


}