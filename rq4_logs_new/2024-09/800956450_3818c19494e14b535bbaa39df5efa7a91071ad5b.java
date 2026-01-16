package io.github.xpakx.images_cdc.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class DebeziumConfig {
    @Value("${debezium.name}")
    private String name;
    @Value("${debezium.connector}")
    private String connectorClass;
    @Value("${debezium.plugin.name}")
    private String pluginName;

    @Value("${db.host}")
    private String dbHost;
    @Value("${db.name}")
    private String dbName;
    @Value("${db.port}")
    private String dbPort;
    @Value("${db.username}")
    private String postgresUsername;
    @Value("${db.password}")
    private String postgresPassword;

    @Value("${debezium.prefix}")
    private String prefix;

    @Bean
    public io.debezium.config.Configuration postgresConnector() {
        Map<String, String> configMap = new HashMap<>();

        configMap.put("name", name);
        configMap.put("connector.class", connectorClass);

        File offsetStorageTempFile = new File("offsets_.dat");
        configMap.put("offset.storage",  "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        configMap.put("offset.storage.file.filename", offsetStorageTempFile.getAbsolutePath());
        configMap.put("offset.flush.interval.ms", "60000");

        configMap.put("database.hostname", dbHost);
        configMap.put("database.server.name", dbHost);
        configMap.put("database.port", dbPort);
        configMap.put("database.user", postgresUsername);
        configMap.put("database.password", postgresPassword);
        configMap.put("database.dbname", dbName);
        configMap.put("plugin.name", pluginName);

        configMap.put("topic.prefix", prefix);

        return io.debezium.config.Configuration.from(configMap);
    }
}