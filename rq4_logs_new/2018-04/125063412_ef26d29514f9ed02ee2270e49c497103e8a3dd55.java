package com.tianyi.bo;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.util.Date;

/**
 * server
 *用于返回同一邀请码其他
 * @author GaoZhilai
 * @date 2018/4/10.
 */
@ApiModel(value="看看大家手气列表元素",description="看看大家手气列表元素")
public class EveryoneListEle {
    @ApiModelProperty(value="用户昵称",name="nickName",example="AIDOC66960261")
    private String nickName;
    @ApiModelProperty(value="奖励的aidoc",name="registAidoc",example="5.0")
    private double registAidoc;
    private Date createdOn;

    public String getNickName() {
        return nickName;
    }

    public void setNickName(String nickName) {
        this.nickName = nickName;
    }

    public double getRegistAidoc() {
        return registAidoc;
    }

    public void setRegistAidoc(double registAidoc) {
        this.registAidoc = registAidoc;
    }

    public Date getCreatedOn() {
        return createdOn;
    }

    public void setCreatedOn(Date createdOn) {
        this.createdOn = createdOn;
    }
}