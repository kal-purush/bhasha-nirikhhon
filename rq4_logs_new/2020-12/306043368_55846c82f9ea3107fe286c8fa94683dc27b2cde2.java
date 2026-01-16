package org.example.module.system.server.domain.entity;

import org.example.common.util.JsonUtil;

/**
 * 系统信息
 */
public class SysInfo {

    /**
     * 操作系统名
     */
    private String osName;

    /**
     * 操作系统架构
     */
    private String osArch;

    public String getOsName() {
        return osName;
    }

    public void setOsName(String osName) {
        this.osName = osName;
    }

    public String getOsArch() {
        return osArch;
    }

    public void setOsArch(String osArch) {
        this.osArch = osArch;
    }

    @Override
    public String toString() {
        return JsonUtil.toJSONString(this);
    }
}