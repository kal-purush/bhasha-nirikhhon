package com.university.contractors.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

@Configuration
public class DataSourceConfiguration {

    private final DataSourceConfigurationReader dataSourceConfigurationReader;

    @Autowired
    public DataSourceConfiguration(DataSourceConfigurationReader dataSourceConfigurationReader) {
        this.dataSourceConfigurationReader = dataSourceConfigurationReader;
    }

    @Bean
    public DataSource dataSource() {
        return DataSourceBuilder
                .create()
                .username(dataSourceConfigurationReader.getUsername())
                .password(dataSourceConfigurationReader.getPassword())
                .url(dataSourceConfigurationReader.getUrl())
                .driverClassName(dataSourceConfigurationReader.getDriverName())
                .build();
    }
}