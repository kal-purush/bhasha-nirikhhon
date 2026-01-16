package org.example.module.system.server.domain.entity;

import org.example.common.util.JsonUtil;

/**
 * CPU 信息
 */
public class CpuInfo {

    /**
     * CPU 核心数
     */
    private int coreNum;
    /**
     * CPU 系统使用率
     */
    private double sysUsage;
    /**
     * CPU 用户使用率
     */
    private double userUsage;
    /**
     * CPU 当前等待率
     */
    private double ioWaitRate;
    /**
     * CPU 当前空闲率
     */
    private double freeRate;

    public int getCoreNum() {
        return coreNum;
    }

    public void setCoreNum(int coreNum) {
        this.coreNum = coreNum;
    }


    public double getSysUsage() {
        return sysUsage;
    }

    public void setSysUsage(double sysUsage) {
        this.sysUsage = sysUsage;
    }

    public double getUserUsage() {
        return userUsage;
    }

    public void setUserUsage(double userUsage) {
        this.userUsage = userUsage;
    }

    public double getIoWaitRate() {
        return ioWaitRate;
    }

    public void setIoWaitRate(double ioWaitRate) {
        this.ioWaitRate = ioWaitRate;
    }

    public double getFreeRate() {
        return freeRate;
    }

    public void setFreeRate(double freeRate) {
        this.freeRate = freeRate;
    }

    @Override
    public String toString() {
        return JsonUtil.toJSONString(this);
    }
}