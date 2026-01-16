package com.dungeongroupfinder;

import com.dungeongroupfinder.repository.GuildRepository;
import com.dungeongroupfinder.repository.PlayerRepository;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.MySQLContainer;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

@Sql(scripts = {"/players_schema.sql", "/guilds_schema.sql"})
@RunWith(SpringRunner.class)
@AutoConfigureMockMvc
@SpringBootTest
@ContextConfiguration(initializers = {GuildFinderIntegrationTest.Initializer.class})
public class GuildFinderIntegrationTest {

    @ClassRule
    public static MySQLContainer mySQLContainer = new MySQLContainer("mysql:8.0.21")
            .withUsername("root")
            .withPassword("");
            //default DB name - test

    static class Initializer
            implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
            TestPropertyValues.of(
                    "spring.datasource.url=" + mySQLContainer.getJdbcUrl(),
                    "spring.datasource.username=" + mySQLContainer.getUsername(),
                    "spring.datasource.password=" + mySQLContainer.getPassword()
            ).applyTo(configurableApplicationContext.getEnvironment());
        }
    }

    @Autowired
    protected MockMvc mvc;

    @Autowired
    protected PlayerRepository playerRepository;

    @Autowired
    protected GuildRepository guildRepository;

}