package ua.ithillel.lesson29;

import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import ua.ithillel.lesson22.HeroDao;
import ua.ithillel.lesson22.HeroDaoImpl;
import ua.ithillel.lesson23.HeroMovieService;
import ua.ithillel.lesson23.HeroService;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
@ComponentScan("lesson29")
@PropertySource("classpath:application.properties")
public class HeroConfiguration {
    @Value("${databaseName}")
    private String databaseName;

    @Value("${user}")
    private String user;

    @Value("${password}")
    private String password;

    @Bean
    public DataSource createDataSource(){
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setDatabaseName(databaseName);
        dataSource.setUser(user);
        dataSource.setPassword(password);

        return dataSource;
    }

    @Bean
    public HeroDao createHeroDao(DataSource dataSource){
        return new HeroDaoImpl(dataSource);
    }

    @Bean
    public HeroMovieService createHeroMovieService(){
        Map<String, List<String>> heroMovies = new HashMap<>();
        // --- заполнение heroMovies списками фильмов для каждого героя
        return new HeroMovieService(heroMovies);
    }

    @Bean
    public HeroService createService(HeroDao heroDao, HeroMovieService heroMovieService) {
        return new HeroService(heroDao, heroMovieService);
    }
}