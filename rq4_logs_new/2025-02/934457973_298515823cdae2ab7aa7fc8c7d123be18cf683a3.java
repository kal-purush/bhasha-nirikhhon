package com.proyectofinal.clave_compas.bd.clavecompas;

import com.zaxxer.hikari.HikariDataSource;
import jakarta.persistence.EntityManagerFactory;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.boot.orm.jpa.EntityManagerFactoryBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableTransactionManagement
@EnableJpaRepositories(basePackages = "com.proyectofinal.clave_compas.bd.clavecompas.repositories",
        entityManagerFactoryRef = "clavecompasEntityManagerFactory",
        transactionManagerRef = "txManagerClavecompas")
public class BDConfig {

    // CONEXION CON LA BASE DE DATOS SEGURIDAD
    @Bean(name = "cxClavecompasDS")
    @ConfigurationProperties(prefix = "spring.datasource")
    DataSource clavecompasDataSource() throws NamingException {
        return DataSourceBuilder.create().type(HikariDataSource.class).build();
    }

    @Bean(name = "clavecompasEntityManagerFactory")
    @Primary
    LocalContainerEntityManagerFactoryBean clavecompasEntityManagerFactory(
            EntityManagerFactoryBuilder builder, @Qualifier("cxClavecompasDS") DataSource dataSource) {
        return builder.dataSource(dataSource)
                .packages("com.proyectofinal.clave_compas.bd.clavecompas.entities")
                .persistenceUnit("clavecompas").properties(getHibernateProperties()).build();
    }

    //Para usar transacciones
    @Bean(name = "txManagerClavecompas")
    PlatformTransactionManager seguridadTransactionManager(
            @Qualifier("clavecompasEntityManagerFactory") EntityManagerFactory entityManagerFactory) {
        return new JpaTransactionManager(entityManagerFactory);
    }

    //Para usar hibernate con SessionFactory
    @Bean(name = "sessionClavecompas")
    SessionFactory sessionFactoryclavecompas(
            @Qualifier("clavecompasEntityManagerFactory") EntityManagerFactory emf) {
        if (emf.unwrap(SessionFactory.class) == null) {
            throw new NullPointerException("clavecompas factory is not a hibernate factory");
        }
        return emf.unwrap(SessionFactory.class);
    }

    private Map<String, Object> getHibernateProperties() {
        Map<String, Object> properties = new HashMap<>();
        //properties.put("hibernate.dialect", "org.hibernate.dialect.PostgreSQLDialect");
        properties.put("hibernate.show_sql", "true");
        properties.put("hibernate.format_sql", "true");
        properties.put("hibernate.connection.release_mode", "AFTER_TRANSACTION");
        properties.put("hibernate.type", "true");
        return properties;
    }
}