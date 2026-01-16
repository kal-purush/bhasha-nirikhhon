package org.example.repository;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class BaseRepositoryTest {

    protected static final PostgreSQLContainer<?> postgreSQLContainer;

    static {
        postgreSQLContainer = new PostgreSQLContainer<>("postgres:latest")
                .withDatabaseName("postgres")
                .withUsername("root")
                .withPassword("root");
        postgreSQLContainer.start();

        // Set system properties for the database connection
        System.setProperty("database.url", postgreSQLContainer.getJdbcUrl());
        System.setProperty("database.username", postgreSQLContainer.getUsername());
        System.setProperty("database.password", postgreSQLContainer.getPassword());
    }

    @BeforeAll
    public void setUp() {
    }

    @AfterAll
    public void tearDown() {
        // Stop the container after all tests are done
        postgreSQLContainer.stop();
    }
}