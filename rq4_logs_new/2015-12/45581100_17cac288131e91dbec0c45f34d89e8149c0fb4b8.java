package incircle.config.DataSource;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/**
 * Created by Julian on 12/12/2015.
 */
@Configuration
@Profile("dev")
public class DevDataSourceConfig {
    @Bean
    public DriverManagerDataSource dataSource() {
        DriverManagerDataSource driverManagerDataSource = new DriverManagerDataSource();
        driverManagerDataSource.setDriverClassName("org.postgresql.Driver");
        driverManagerDataSource.setUrl("jdbc:postgresql://localhost:5432/testIncircle");
        driverManagerDataSource.setUsername("julian");
        driverManagerDataSource.setPassword("juls");
        return driverManagerDataSource;
    }
}