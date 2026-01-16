package com.springtest.demo.config.typefilter;

import org.springframework.core.io.Resource;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.core.type.ClassMetadata;
import org.springframework.core.type.classreading.MetadataReader;
import org.springframework.core.type.classreading.MetadataReaderFactory;
import org.springframework.core.type.filter.TypeFilter;

import java.io.IOException;

public class PersonTypeFilter implements TypeFilter {
    /**
     *
     * @param metadataReader 读取当前正在被扫描的类的信息
     * @param metadataReaderFactory MetadataReader的工厂类，用于获取其他类
     * @return
     * @throws IOException
     */
    @Override
    public boolean match(MetadataReader metadataReader, MetadataReaderFactory metadataReaderFactory) throws IOException {
        // 获取当前被扫描的类的注解信息
        AnnotationMetadata annotationMetadata =  metadataReader.getAnnotationMetadata();
        // 获取当前被扫描的类的类信息
        ClassMetadata classMetadata = metadataReader.getClassMetadata();
        // 自定义规则可以随意定义包的扫描规则，譬如这里让没有相关注解的PersonTypeFilter自身被扫描注册
        if (classMetadata.getClassName().contains("filter")) {
            return true;
        }
        // System.out.println("getEnclosingClassName------>"+classMetadata.getEnclosingClassName());
        // System.out.println("getClassName         ------>"+classMetadata.getClassName());
        // 获取当前被扫描的类的资源信息（类的路径信息）
        Resource resource = metadataReader.getResource();
        return false;
    }
}