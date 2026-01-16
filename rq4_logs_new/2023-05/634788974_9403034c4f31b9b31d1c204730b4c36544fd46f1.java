import org.testcontainers.containers.PostgreSQLContainer;

public class PostgresContainerWrapper extends PostgreSQLContainer<PostgresContainerWrapper> {
    private static final String POSTGRES_IMAGE_NAME = "postgres:11";
    private static final String POSTGRES_DB = "integtest_users_tests_db";
    private static final String POSTGRES_USER = "test";
    private static final String POSTGRES_PASSWORD = "test";

    public PostgresContainerWrapper() {
        super(POSTGRES_IMAGE_NAME);
        this
                .withDatabaseName(POSTGRES_DB)
                .withUsername(POSTGRES_USER)
                .withPassword(POSTGRES_PASSWORD)
                .withInitScript("init-db-schema-integTest.sql");
    }
}