package com.huiyuan2.cloud.common.properties;

import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.core.env.PropertiesPropertySource;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.support.EncodedResource;
import org.springframework.core.io.support.PropertySourceFactory;
import org.springframework.lang.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 * @description: 加载yml格式的自定义配置文件
 * @author： 灰原二
 * @date: 2022/9/24 21:07
 */
public class YamlPropertyLoader implements PropertySourceFactory {
    @Override
    public PropertySource<?> createPropertySource(@Nullable String name, EncodedResource encodedResource) throws IOException {
        Properties properties = loadYaml2Properties(encodedResource);
        String sourceName = name !=null?name:encodedResource.getResource().getFilename();

        assert sourceName !=null;
        return new PropertiesPropertySource(sourceName,properties);
    }

    /**
     * 加载yaml文件并转换为properties
     * @param resource
     * @return
     * @throws FileNotFoundException
     */
    private Properties loadYaml2Properties(EncodedResource resource) throws FileNotFoundException{
        try{
            YamlPropertiesFactoryBean yamlPropertiesFactoryBean = new YamlPropertiesFactoryBean();
            yamlPropertiesFactoryBean.setResources(resource.getResource());
            yamlPropertiesFactoryBean.afterPropertiesSet();
            return yamlPropertiesFactoryBean.getObject();
        }catch (IllegalStateException e){
            Throwable cause = e.getCause();
            if(cause instanceof FileNotFoundException){
                throw (FileNotFoundException) e.getCause();
            }
            throw e;
        }

    }
}