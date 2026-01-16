package com.tqs108636.busservicebackend.config;

import java.lang.invoke.MethodHandles;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.stereotype.Component;

// Runs on load to insert data on database if it's empty
@Component
public class DatabaseInitializer {
    private DataSource dataSource;

    private JdbcTemplate jdbcTemplate;

    private Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().getClass());

    @Autowired
    public DatabaseInitializer(DataSource dataSource, JdbcTemplate jdbcTemplate) {
        this.dataSource = dataSource;
        this.jdbcTemplate = jdbcTemplate;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void initializeDatabase() {
        ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
        populator.addScript(new ClassPathResource("init-data.sql"));

        String query = "SELECT COUNT(*) FROM location";

        try {
            logger.info("---- DATABASE INITIALIZATION START ----");
            int count = jdbcTemplate.queryForObject(query, Integer.class);
            logger.debug("Size of location table - {}", count);
            if (count == 0) {
                logger.info("Database is empty, initializing database with initial data defined in 'init-data.sql'");
                populator.populate(dataSource.getConnection());
                logger.info("Finished initializing database...");
            }
            logger.info("Database already had data...");
        } catch (Exception e) {
            logger.error("Failed to initialize database with \"init-data.sql\"");
        }
        logger.info("---- DATABASE INITIALIZATION DONE ----");
    }
}