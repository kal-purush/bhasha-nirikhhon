package com.example.configclient.service;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.EurekaClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ConfigClientService {

    @Autowired
    private EurekaClient discoveryClient;

    public String serviceUrl(String applicationName) {
        InstanceInfo instance = discoveryClient.getNextServerFromEureka(applicationName, false);
        return instance.getHomePageUrl();
    }

}