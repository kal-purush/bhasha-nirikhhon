package com.akai.noder.configs;

import org.flywaydb.core.Flyway;
import org.springframework.boot.autoconfigure.flyway.FlywayMigrationStrategy;
import org.springframework.context.annotation.Configuration;

@Configuration
public class NoderFlywayMigrationStrategy implements FlywayMigrationStrategy {

    @Override
    public void migrate(Flyway flyway) {
        flyway.setSchemas("public");
        flyway.setTable("schema_version");
        flyway.setLocations("classpath:db/migrations");
        flyway.migrate();
    }
}