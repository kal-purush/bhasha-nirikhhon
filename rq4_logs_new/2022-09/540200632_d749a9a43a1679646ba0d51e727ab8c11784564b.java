package com.huiyuan2.cloud.basic.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * @description: 服务实例信息
 * @author： 灰原二
 * @date: 2022/9/26 7:20
 */
@Slf4j
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CloudInstance {

    /**
     * 服务名称
     */
    private String serviceName;


    private String ip;;

    private int port;

    /**
     * 用户自定义元数据
     */
    private Map<String,String> customizeMetadata = new HashMap<>();

    /**
     * 心跳时间
     */
    private long renewTime;


    /**
     * 唯一标识
     * @return
     */
    public String getUniqueIdentity(){
        return serviceName+":"+ip+":"+port;
    }

    /**
     * 刷新心跳时间
     */
    public void renew(){
        this.renewTime = System.currentTimeMillis();
    }

    /**
     * 判断是否超时
     * @param timeoutThreshold 超时阈值
     * @return
     */
    public boolean isTimeout(long timeoutThreshold){
        return this.renewTime+timeoutThreshold<System.currentTimeMillis();
    }

    @Override
    public boolean equals(Object o){
        if(this ==o) return  true;
        if(o ==null ||getClass()!=o.getClass()) return false;
        CloudInstance target = (CloudInstance)o;
        return Objects.equals(getUniqueIdentity(),target.getUniqueIdentity());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getUniqueIdentity());
    }
}