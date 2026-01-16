package com.autohome.entity;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.io.Serializable;
import lombok.Data;

@ApiModel(value = "com-autohome-entity-Address")
@Data
public class Address implements Serializable {
    /**
     *
     */
    @ApiModelProperty(value = "")
    private Integer aid;

    /**
     * 用户编号-外键
     */
    @ApiModelProperty(value = "用户编号-外键")
    private Integer uid;

    /**
     * 联系人姓名
     */
    @ApiModelProperty(value = "联系人姓名")
    private String name;

    /**
     * 地区
     */
    @ApiModelProperty(value = "地区")
    private String region;

    /**
     * 街道地址
     */
    @ApiModelProperty(value = "街道地址")
    private String address;

    /**
     * 邮政编码
     */
    @ApiModelProperty(value = "邮政编码")
    private String postal;

    /**
     * 手机号码
     */
    @ApiModelProperty(value = "手机号码")
    private String phone;

    /**
     * 电话号码
     */
    @ApiModelProperty(value = "电话号码")
    private String telephone;

    /**
     * 默认地址标记 1默认 0不是默认
     */
    @ApiModelProperty(value = "默认地址标记 1默认 0不是默认")
    private Integer default_address;

    private static final long serialVersionUID = 1L;
}
