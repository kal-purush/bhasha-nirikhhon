package com.joizhang.naiverpc.spring.boot.autoconfigure;

import com.joizhang.naiverpc.config.ApplicationConfig;
import com.joizhang.naiverpc.spring.boot.util.NaiveRpcUtils;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

@ConfigurationProperties(prefix = NaiveRpcUtils.NAIVE_RPC_PREFIX)
public class NaiveRpcConfigurationProperties {

    @NestedConfigurationProperty
    private ApplicationConfig application = new ApplicationConfig();

}