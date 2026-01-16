package com.numo.batch.conf;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;

@AutoConfiguration
@ConditionalOnProperty(prefix = "spring.datasource-meta", name = "jdbc-url")
@Import({BatchConfig.class, MetaDBConfig.class})
@ComponentScan(basePackages = "com.numo.batch")
public class BatchAutoConfig {
}