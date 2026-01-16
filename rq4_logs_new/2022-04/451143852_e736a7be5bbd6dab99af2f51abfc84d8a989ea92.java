package ru.idgroup.otus.jdbc.mapper;


import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import ru.idgroup.otus.jdbc.HomeWork;
import ru.idgroup.otus.jdbc.core.repository.executor.DbExecutor;
import ru.idgroup.otus.jdbc.core.repository.executor.DbExecutorImpl;
import ru.idgroup.otus.jdbc.core.sessionmanager.TransactionRunnerJdbc;
import ru.idgroup.otus.jdbc.crm.datasource.DriverManagerDataSource;
import ru.idgroup.otus.jdbc.crm.model.Client;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class DataTemplateJdbcTest {

    private static final Logger log = LoggerFactory.getLogger(HomeWork.class);
    private static final String USER = "owner";
    private static final String PASSWORD = "secret";

    private EntityClassMetaDataImpl<Client> entityClassMetaDataClient;
    private EntitySQLMetaDataImpl entitySQLMetaDataClient;
    private DbExecutor dbExecutor;
    private DataTemplateJdbc<Client> dataTemplateClient;

    @Container
    private final PostgreSQLContainer<?> postgresqlContainer = new PostgreSQLContainer<>("postgres:12-alpine")
            .withDatabaseName("testDB")
            .withUsername(USER)
            .withPassword(PASSWORD);

    @BeforeEach
    void setUp() {
        var dataSource = new DriverManagerDataSource(postgresqlContainer.getJdbcUrl(), USER, PASSWORD);
        flywayMigrations(dataSource);

        entityClassMetaDataClient = new EntityClassMetaDataImpl<>(Client.class);
        entitySQLMetaDataClient = new EntitySQLMetaDataImpl(entityClassMetaDataClient);
        dbExecutor = new DbExecutorImpl();
        dataTemplateClient = new DataTemplateJdbc<>(dbExecutor, entityClassMetaDataClient, entitySQLMetaDataClient);
    }

    @Test
    void findByIdTest() throws SQLException {
        //given
        Client client = new Client(1l, "test");

        //when
        var optClient = dataTemplateClient.findById(makeSingleConnection(), 1);

        //then
        assertThat(optClient.get().toString()).isEqualTo(client.toString());
    }

    @Test
    void findAll() throws SQLException {
        //given
        Client client = new Client(1l, "test");

        //when
        List<Client> clients = dataTemplateClient.findAll(makeSingleConnection());

        //then
        assertThat(clients.size()).isEqualTo(2);
    }

    @Test
    void insert() throws SQLException {
        //given
        Client client = new Client(null, "test2");

        //when
        var connection =  makeSingleConnection();
        var id = dataTemplateClient.insert(connection, client);

        //then
        Client exClient = new Client(id, "test2");
        Optional<Client> resClient = dataTemplateClient.findById(connection, id);

        assertThat(resClient).isPresent();
        assertThat(resClient.get().toString()).isEqualTo(exClient.toString());

    }

    @Test
    void update() throws SQLException {
        //given
        Client client = new Client(1l, "updated test");

        //when
        var connection =  makeSingleConnection();
        dataTemplateClient.update(connection, client);

        //then
        Optional<Client> resClient = dataTemplateClient.findById(connection, 1l);

        assertThat(resClient).isPresent();
        assertThat(resClient.get().toString()).isEqualTo(client.toString());
    }

    private Properties getConnectionProperties() {
        Properties props = new Properties();
        props.setProperty("user", postgresqlContainer.getUsername());
        props.setProperty("password", postgresqlContainer.getPassword());
        props.setProperty("ssl", "false");
        return props;
    }

    private Connection makeSingleConnection() throws SQLException {
        Connection connection = DriverManager.getConnection(postgresqlContainer.getJdbcUrl(), getConnectionProperties());
        connection.setAutoCommit(false);
        return connection;
    }

    private static void flywayMigrations(DataSource dataSource) {
        log.info("db migration started...");
        var flyway = Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:test/db/migration")
                .load();
        flyway.migrate();
        log.info("db migration finished.");
        log.info("***");
    }
}