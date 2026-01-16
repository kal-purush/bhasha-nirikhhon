package lk.server.cocktails.configuration.db;

import org.hibernate.boot.SchemaAutoTooling;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.dialect.PostgreSQL82Dialect;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.Database;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;

import javax.sql.DataSource;
import java.util.Properties;

@Configuration
@EnableJpaRepositories(basePackages = {
        "lk.server.cocktails.features.cocktail.repositories",
        "lk.server.cocktails.features.ingredient.repositories",
        "lk.server.cocktails.features.modes.repositories"
})
public class PostgreSqlConfiguration {

    @Bean(name = "mainJpaDataSource")
    public DataSource dataSource() {
        DriverManagerDataSource driver = new DriverManagerDataSource();
        driver.setDriverClassName("org.postgresql.Driver");
        driver.setUrl("jdbc:postgresql://localhost:5432/test_cocktails_1");
        driver.setUsername("root");
        driver.setPassword("5005");
        return driver;
    }

    @Bean
    public LocalContainerEntityManagerFactoryBean entityManagerFactory(
            @Qualifier("mainJpaDataSource") DataSource dataSource,
            @Qualifier("mainJpaHibernateJpaVendorAdapter") HibernateJpaVendorAdapter adapter,
            @Qualifier("mainJpaProperties") Properties properties,
            @Qualifier("mainJpaModelPaths") String[] modelPaths) {
        LocalContainerEntityManagerFactoryBean factory = new LocalContainerEntityManagerFactoryBean();
        factory.setPackagesToScan(modelPaths);
        factory.setDataSource(dataSource);
        factory.setJpaVendorAdapter(adapter);
        factory.setJpaProperties(properties);
        return factory;
    }

    @Bean(name = "mainJpaModelPaths")
    public String[] jpaModelPaths() {
        return new String[]{
                "lk.server.cocktails.features.cocktail.entities",
                "lk.server.cocktails.features.ingredient.entities",
                "lk.server.cocktails.features.modes.entities"
        };
    }

    @Bean(name = "mainJpaHibernateJpaVendorAdapter")
    public HibernateJpaVendorAdapter jpaHibernateJpaVendorAdapter() {
        HibernateJpaVendorAdapter hibernateJpaVendorAdapter = new HibernateJpaVendorAdapter();
        hibernateJpaVendorAdapter.setDatabasePlatform(PostgreSQL82Dialect.class.getName());
        hibernateJpaVendorAdapter.setDatabase(Database.POSTGRESQL);
//        hibernateJpaVendorAdapter.setGenerateDdl(false);
        hibernateJpaVendorAdapter.setShowSql(false);
        return hibernateJpaVendorAdapter;
    }

    @Bean(name = "mainJpaProperties")
    public Properties jpaProperties() {
        Properties properties = new Properties();
        properties.put(AvailableSettings.HBM2DDL_AUTO, getDDL(SchemaAutoTooling.CREATE));
        properties.put(AvailableSettings.DIALECT, PostgreSQL82Dialect.class.getName());
        return properties;
    }

    private String getDDL(SchemaAutoTooling schemaAutoTooling) {
        return schemaAutoTooling.name().replace("_", "-").toLowerCase();
    }

}