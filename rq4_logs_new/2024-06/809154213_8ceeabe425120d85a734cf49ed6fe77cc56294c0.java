package de.ait_tr.g_40_shop.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

@Configuration
public class DigitalOceanDataSourceConfig {
    @Value("${DB_USERNAME}")
    private String username;
    @Value("${DB_PASSWORD}")
    private String password;
    @Value("${DB_HOST}")
    private String hostname;
    @Value("${DB_PORT}")
    private String port;
    @Value("${DB_NAME}")
    private String database;

    @Bean
    public DataSource dataSource() {
        return DataSourceBuilder.create()
                .driverClassName("org.postgresql.Driver")
                .url("jdbc:postgresql://" + hostname + ":" + port + "/" + database)
                .username(username)
                .password(password)
                .build(); // теперь объект dataSource знает все об удаленнной БД (адрес, имя польз, пароль)
    }
}