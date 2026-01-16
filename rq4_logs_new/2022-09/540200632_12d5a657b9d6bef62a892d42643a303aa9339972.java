package com.huiyuan2.cloud.common.api;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @description:
 * @author： 灰原二
 * @date: 2022/9/25 10:27
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class CommonError implements Serializable {
    private static final long serialVersionUID = -8356735485729498537L;

    /**
     * 错误码
     */
    private Integer errCode;

    /**
     * 错误描述
     */
    private String errMsg;


}