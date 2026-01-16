package com.huiyuan2.cloud.server.instance;

import com.huiyuan2.cloud.basic.domain.MetadataChangedEventEnum;
import com.huiyuan2.cloud.basic.network.proto.MetadataRequest;
import com.huiyuan2.cloud.basic.utils.Constants;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @description: 配置中心 TODO 简易实现
 * @author： 灰原二
 * @date: 2022/9/30 20:00
 */
public class ConfigCenterDistributedServer extends AbstractDistributeServer{

    private final Map<String,String> configMap = new ConcurrentHashMap<>();

    private List<DistributedServerChangedListener> listeners =  new ArrayList<>();

    @Override
    public String serviceType() {
        return Constants.CONFIG_CENTER_TYPE;
    }

    @Override
    public void addMetadata(String metadataKey, String metadataValue) {
        configMap.put(metadataKey,metadataValue);
        invokeListener(metadataKey,metadataValue, MetadataChangedEventEnum.ADD);
    }

    @Override
    public void removeMetadata(String metadataKey) {
        String removeValue = configMap.remove(metadataKey);
        invokeListener(metadataKey,removeValue, MetadataChangedEventEnum.REMOVE);
    }

    @Override
    public Map<String, String> fetchMeatdata(List<String> metadataKeys) {
        return new HashMap<>(configMap);
    }

    @Override
    public void clearMetadata() {
        configMap.clear();
        for (DistributedServerChangedListener listener : listeners) {
            listener.onClearMetadata(serviceType());
        }
    }

    @Override
    public void heartbeat(MetadataRequest metadataRequest) {
        super.heartbeat(metadataRequest);
    }
}