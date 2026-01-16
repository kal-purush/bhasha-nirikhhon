package hse.se.aaizmaylov.petrinetscalculationserver.configuration;

import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.nio.file.Paths;

@Configuration
public class DataBaseConfiguration {
    @Bean
    public DataSource getDataSource() {
        DataSourceBuilder dataSourceBuilder = DataSourceBuilder.create();

        dataSourceBuilder.url("jdbc:h2:" + Paths.get("").toAbsolutePath().resolve("db"));

        return dataSourceBuilder.build();
    }
}