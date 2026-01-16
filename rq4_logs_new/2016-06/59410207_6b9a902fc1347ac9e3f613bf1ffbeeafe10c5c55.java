package pl.joinin.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import javax.sql.DataSource;
import java.net.URI;
import java.net.URISyntaxException;

public abstract class DatabaseConfig {

    protected void configureDataSource(org.apache.tomcat.jdbc.pool.DataSource dataSource) {
        dataSource.setMaxActive(20);
        dataSource.setMaxIdle(8);
        dataSource.setMinIdle(8);
        dataSource.setTestOnBorrow(false);
        dataSource.setTestOnReturn(false);
        dataSource.setValidationQuery("SELECT 1");
    }
}

@Configuration
@Profile("dev")
class DevDatabaseConfig extends DatabaseConfig {
	
	@Value("${spring.datasource.url}")
    private String databaseUrl;
	
	@Value("${spring.datasource.username}")
    private String databaseUsername;
	
	@Value("${spring.datasource.password}")
    private String databasePassword;

    @Bean
    public DataSource dataSource() {
        org.apache.tomcat.jdbc.pool.DataSource dataSource = new org.apache.tomcat.jdbc.pool.DataSource();

        dataSource.setDriverClassName("org.postgresql.Driver");
        dataSource.setUrl(databaseUrl);
        dataSource.setUsername(databaseUsername);
        dataSource.setPassword(databasePassword);

        configureDataSource(dataSource);

        return dataSource;
    }
}

@Configuration
@Profile("heroku")
class HerokuDatabaseConfig extends DatabaseConfig {

    @Value("${spring.datasource.uri}")
    private String databaseUri;

    @Bean
    public DataSource dataSource() throws URISyntaxException {
        org.apache.tomcat.jdbc.pool.DataSource dataSource = new org.apache.tomcat.jdbc.pool.DataSource();

        URI dbUri = new URI(databaseUri);

        dataSource.setUsername(dbUri.getUserInfo().split(":")[0]);
        dataSource.setPassword(dbUri.getUserInfo().split(":")[1]);
        dataSource.setUrl("jdbc:postgresql://" + dbUri.getHost() + ':' + dbUri.getPort() + dbUri.getPath());

        configureDataSource(dataSource);

        return dataSource;
    }
}