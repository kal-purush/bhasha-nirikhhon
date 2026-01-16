package org.example.module.system.server.domain.entity;

import org.example.common.util.JsonUtil;

/**
 * 文件存储信息
 */
public class FileStoreInfo {

    /**
     * 挂载路径
     */
    private String mount;
    /**
     * 挂载类型
     */
    private String type;
    /**
     * 文件类型
     */
    private String name;
    /**
     * 总大小
     */
    private long total;
    /**
     * 剩余大小
     */
    private long available;
    /**
     * 已使用
     */
    private long used;

    public String getMount() {
        return mount;
    }

    public void setMount(String mount) {
        this.mount = mount;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }

    public long getAvailable() {
        return available;
    }

    public void setAvailable(long available) {
        this.available = available;
    }

    public long getUsed() {
        return used;
    }

    public void setUsed(long used) {
        this.used = used;
    }

    @Override
    public String toString() {
        return JsonUtil.toJSONString(this);
    }
}