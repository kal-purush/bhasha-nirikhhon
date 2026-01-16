package org.cyberrealm.tech.config;

import org.testcontainers.containers.PostgreSQLContainer;

public class CustomPostgreSqlContainer extends PostgreSQLContainer<CustomPostgreSqlContainer> {
    private static final String DB_IMAGE = "postgres:16-alpine";
    private static CustomPostgreSqlContainer postgreSQLContainer;

    private CustomPostgreSqlContainer(String dockerImageName) {
        super(dockerImageName);
    }

    public static synchronized CustomPostgreSqlContainer getInstance() {
        if (postgreSQLContainer == null) {
            postgreSQLContainer = new CustomPostgreSqlContainer(DB_IMAGE);
        }
        return postgreSQLContainer;
    }

    @Override
    public void start() {
        super.start();
        System.setProperty("TEST_DB_URL", postgreSQLContainer.getJdbcUrl());
        System.setProperty("TEST_DB_USERNAME", postgreSQLContainer.getUsername());
        System.setProperty("TEST_DB_PASSWORD", postgreSQLContainer.getPassword());
    }

    @Override
    public void stop() {
        super.stop();
    }
}