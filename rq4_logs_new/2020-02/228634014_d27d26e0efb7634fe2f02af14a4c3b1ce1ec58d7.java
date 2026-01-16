package net.tch.user.abs;

import java.io.Serializable;

/**
 * @description:抽象返回数据类，定义了每次返回都要返回的字段
 * @auth tongchenghao
 * @date 2020/2/25
 */
public class AbstractResponse implements Serializable {
    private static final long serialVersionUID = 21487925870461091L;

    protected String code;
    protected String message;

    protected String token;

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    @Override
    public String toString() {
        return "AbstractResponse{" +
                "code='" + code + '\'' +
                ", message='" + message + '\'' +
                ", token='" + token + '\'' +
                '}';
    }
}