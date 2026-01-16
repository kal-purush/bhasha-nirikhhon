package com.xuanwu.ai.base.response;

import com.alibaba.fastjson.annotation.JSONField;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.xuanwu.ai.base.exception.enums.ResponseEnum;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpStatus;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.List;

/**
 * <p>
 * REST API 返回结果
 * </p>
 *
 * @author gourd.hu
 * @since 2018-11-08
 */
@Data
@Accessors(chain = true)
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class BaseResponse<T> implements Serializable {

    @ApiModelProperty("响应码")
    private Integer code;

    @ApiModelProperty("响应消息")
    private String message;

    @ApiModelProperty(value = "traceId")
    private String traceId;

    @ApiModelProperty("响应数据")
    private T data;

    private static BaseResponse<Boolean> result(ResponseEnum responseEnum){
        return result(responseEnum,null);
    }

    private static <T> BaseResponse<T> result(ResponseEnum responseEnum, T data){
        return result(responseEnum,null,data,null);
    }

    private static <T> BaseResponse<T> result(ResponseEnum responseEnum, String message, T data, List<String> errors){
        boolean success = false;
        if (responseEnum.getCode() == HttpStatus.OK.value()){
            success = true;
        }
        String apiMessage = responseEnum.getMessage();
        if (StringUtils.isBlank(message)){
            message = apiMessage;
        }
        return (BaseResponse<T>) BaseResponse.builder()
                .code(responseEnum.getCode())
                .message(message)
                .data(data)
                .build();
    }

    public static BaseResponse<Boolean> ok(){
        return ok(null);
    }

    public static <T> BaseResponse<T> ok(T data){
        return result(ResponseEnum.OK,data);
    }

    public static <T> BaseResponse<T> ok(String message, T data ){
        return result(ResponseEnum.OK,message,data,null);
    }

    public static BaseResponse<Boolean> fail() {
        return fail(ResponseEnum.INTERNAL_SERVER_ERROR);
    }

    public static BaseResponse<Boolean> fail(ResponseEnum responseEnum){
        return result(responseEnum,null);
    }

    public static BaseResponse<String> fail(String message){
        return result(ResponseEnum.INTERNAL_SERVER_ERROR,message,null,null);

    }

    public static BaseResponse<String> fail(ResponseEnum responseEnum, String message){
        return result(responseEnum,message,null,null);
    }

    public static BaseResponse<String> fail(ResponseEnum responseEnum, String message, List<String> errors){
        return result(responseEnum,message,null,errors);

    }

}