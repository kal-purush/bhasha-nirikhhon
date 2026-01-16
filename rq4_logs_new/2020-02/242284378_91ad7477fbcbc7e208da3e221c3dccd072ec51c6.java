package com.zjb.configurationfile.config;

import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.core.env.PropertiesPropertySource;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.support.DefaultPropertySourceFactory;
import org.springframework.core.io.support.EncodedResource;

import java.io.IOException;
import java.util.Properties;

/**
 * 用于读取yaml或yml类型的文件
 * @author zhaojianbo
 * @date 2020/2/22 21:39
 */
public class YamlPropertyConfig extends DefaultPropertySourceFactory {
    @Override
    public PropertySource<?> createPropertySource(String name, EncodedResource resource) throws IOException {
        String sourceName = (name == null) ? resource.getResource().getFilename() : name;
        assert sourceName != null;
        if (sourceName.endsWith(".yml") || sourceName.endsWith(".yaml")) {
            YamlPropertiesFactoryBean factory = new YamlPropertiesFactoryBean();
            factory.setResources(resource.getResource());
            factory.afterPropertiesSet();
            Properties properties = factory.getObject();
            assert properties != null;
            return new PropertiesPropertySource(sourceName, properties);
        }
        return super.createPropertySource(name, resource);
    }
}