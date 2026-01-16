package org.example.module.system.server.domain.response;

import org.example.common.util.JsonUtil;

/**
 * @author linzhaoming
 * @since 2020/12/12
 **/
public class ServerResponse {

    /** 主机地址 */
    private String hostAddress;

    /** 服务器时间 */
    private String time;

    /** 已运行时间 */
    private String uptime;

    /** 系统内存 */
    private String memory;

    /** CPU 核心数 */
    private Integer coreNum;

    /** 存储空间 */
    private String storage;

    /** CPU 使用率 */
    private double cpuUsage;

    /** 内存使用率 */
    private double memoryUsage;

    /** 最大分区使用率 */
    private double maxSizePartitionUsage;

    public String getHostAddress() {
        return hostAddress;
    }

    public void setHostAddress(String hostAddress) {
        this.hostAddress = hostAddress;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getUptime() {
        return uptime;
    }

    public void setUptime(String uptime) {
        this.uptime = uptime;
    }

    public String getMemory() {
        return memory;
    }

    public void setMemory(String memory) {
        this.memory = memory;
    }

    public Integer getCoreNum() {
        return coreNum;
    }

    public void setCoreNum(Integer coreNum) {
        this.coreNum = coreNum;
    }

    public String getStorage() {
        return storage;
    }

    public void setStorage(String storage) {
        this.storage = storage;
    }

    public double getCpuUsage() {
        return cpuUsage;
    }

    public void setCpuUsage(double cpuUsage) {
        this.cpuUsage = cpuUsage;
    }

    public double getMemoryUsage() {
        return memoryUsage;
    }

    public void setMemoryUsage(double memoryUsage) {
        this.memoryUsage = memoryUsage;
    }

    public double getMaxSizePartitionUsage() {
        return maxSizePartitionUsage;
    }

    public void setMaxSizePartitionUsage(double maxSizePartitionUsage) {
        this.maxSizePartitionUsage = maxSizePartitionUsage;
    }

    @Override
    public String toString() {
        return JsonUtil.toJSONString(this);
    }
}