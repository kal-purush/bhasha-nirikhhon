package ua.ithillel.lesson31;


import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ua.ithillel.lesson23.HeroFabric;
import ua.ithillel.lesson23.HeroServiceDao;

import javax.sql.DataSource;

@Configuration
public class SpringMvcConfig {

    @Bean
    public HeroServiceDao heroService(){
        return HeroFabric.createServiceRest(getDataSource());
    }

    private DataSource getDataSource(){
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setDatabaseName("dbHero");
        dataSource.setUser("postgres");
        dataSource.setPassword("postgres");
        return dataSource;
    }
}