package com.center.api.dto.request;

import com.center.api.base.BaseDto;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiModelProperty;

/**
 * @author heyutang
 * @Title:
 * @Package com.center.api.dto.request
 * @Description:
 * @Company
 * @date 2020/7/8 15:07
 */

@Api(tags = "登录请求接口")
public class LoginRequest extends BaseDto {

    /**
     * 用户名
     */
    @ApiModelProperty(value = "用户名",example = "admin")
    private String username;

    /**
     * 密码
     */
    @ApiModelProperty(value = "密码(base64加密)",example = "sds")
    private String password;

    /**
     * 是否记住我
     */
    @ApiModelProperty(value = "是否记住我:0否，1是",example = "0")
    private Short rememberMe;

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Short getRememberMe() {
        return rememberMe;
    }

    public void setRememberMe(Short rememberMe) {
        this.rememberMe = rememberMe;
    }
}