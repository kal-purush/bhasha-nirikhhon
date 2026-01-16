package com.siatsenko.movieland;

import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.*;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import org.springframework.web.WebApplicationInitializer;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.context.ContextLoaderListener;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;
import org.springframework.web.servlet.DispatcherServlet;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRegistration;

@Configuration
@ComponentScan(basePackages = "com.siatsenko.movieland"
  , excludeFilters = @ComponentScan.Filter(type = FilterType.REGEX, pattern = "com.siatsenko.movieland.controller.*"))
//@ImportResource({"classpath*:spring/root-context.xml"})
@EnableWebMvc
@PropertySource("classpath:application.yml")
public class AppConfiguration implements SchedulingConfigurer
//        , WebApplicationInitializer
{
    private static final Logger logger = LoggerFactory.getLogger(AppConfiguration.class);

    @Autowired
    private Environment environment;

    //    Movies
    @Bean
    public String allMoviesSql() {
        return "SELECT * FROM v_movies /*ORDER BY*/;";
    }

    @Bean
    public String randomMoviesSql() {
        return "SELECT * FROM v_movies ORDER BY random() limit ?;";
    }

    @Bean
    public String moviesByGenreIdSql() {
        return "SELECT vm.* FROM v_movies vm INNER JOIN movie_genre mg ON vm.id = mg.movie_id WHERE mg.genre_id = ? /*ORDER BY*/ ";
    }

    @Bean
    public String movieByIdSql() {
        return "SELECT * FROM v_movies WHERE id = ?;";
    }

    @Bean
    public String addMovieSql() {
        return "INSERT INTO movie (name_russian,name_native,year_of_release,description,rating,price,picture_path) VALUES (:nameRussian,:nameNative,:yearOfRelease,:description,:rating,:price,:picturePath);";
    }

    @Bean
    public String editMovieSql() {
        return "INSERT INTO movie (id,name_russian,name_native,year_of_release,description,rating,price,picture_path) VALUES (:id,:nameRussian,:nameNative,:yearOfRelease,:description,:rating,:price,:picturePath)\n" +
                "            ON CONFLICT ON CONSTRAINT movie_pkey DO\n" +
                "    UPDATE\n" +
                "    SET name_russian = EXCLUDED.name_russian,name_native = EXCLUDED.name_native,year_of_release = EXCLUDED.year_of_release,description = EXCLUDED.description,rating = EXCLUDED.rating,price = EXCLUDED.price,picture_path = EXCLUDED.picture_path;";
    }

    //    Genres
    @Bean
    public String allGenresSql() {
        return "SELECT * FROM genre;";
    }

    @Bean
    public String genresByMovieIdSql() {
        return "SELECT g.* FROM movie_genre mg INNER JOIN genre g ON mg.genre_id = g.id WHERE movie_id = ?;";
    }

    @Bean
    public String editGenresByMovieIdSql() {
        return "WITH del_genres AS ( DELETE FROM movie_genre WHERE movie_id = ?)\n" +
                "INSERT INTO movie_genre (movie_id, genre_id)\n" +
                "SELECT ?, g.id FROM genre g where g.id IN ( SELECT UNNEST(?) );";
    }

    //    Reviews
    @Bean
    public String reviewsByMovieIdSql() {
        return "SELECT r.* FROM review r INNER JOIN users u ON r.user_id = u.id WHERE r.movie_id = ?;";
    }

    @Bean
    public String addReviewSql() {
        return "INSERT INTO review (movie_id, user_id, text) VALUES (?, ?, ?);";
    }


    //    Users
    @Bean
    public String usersByIdsSql() {
        return "SELECT u.* FROM users u WHERE u.id IN ( SELECT UNNEST(?) );";
    }

    @Bean
    public String userByAuthSql() {
        return "SELECT u.* FROM users u WHERE (u.email = ?) AND (u.pswhash = crypt(?, pswhash));";
    }

    //    Countries
    @Bean
    public String allCountriesSql() {
        return "SELECT * FROM country;";
    }

    @Bean
    public String countriesByMovieIdSql() {
        return "SELECT c.* FROM movie_country mc INNER JOIN country c ON mc.country_id = c.id WHERE movie_id = ?;";
    }

    @Bean
    public String editCountriesByMovieIdSql() {
        return "WITH del_countries AS (DELETE FROM movie_country WHERE movie_id = ?)\n" +
                "INSERT INTO movie_country (movie_id, country_id)\n" +
                "SELECT ?, c.id FROM country c where c.id IN ( SELECT UNNEST(?) );";
    }

    @Bean
//@Scope("singleton")
//    public BasicDataSource dataSource() {
//        BasicDataSource dataSource = new BasicDataSource();
//        dataSource.setDriverClassName(environment.getProperty("dbcp2.driverClassName"));
//        dataSource.setUrl(environment.getProperty("dbcp2.url"));
//        dataSource.setUsername(environment.getProperty("dbcp2.username"));
//        dataSource.setPassword(environment.getProperty("dbcp2.password"));
//        dataSource.setDefaultAutoCommit(Boolean.valueOf(environment.getProperty("dbcp2.defaultAutoCommit")));
//        dataSource.setMinIdle(Integer.parseInt(environment.getProperty("dbcp2.minIdle")));
//        dataSource.setMaxIdle(Integer.parseInt(environment.getProperty("dbcp2.maxIdle")));
//        dataSource.setMaxOpenPreparedStatements(Integer.parseInt(environment.getProperty("dbcp2.maxOpenPreparedStatements")));
//        return dataSource;
//    }
    public BasicDataSource dataSource() {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName(environment.getProperty("spring.datasource.dbcp2.driver-class-name"));
        dataSource.setUrl(environment.getProperty("spring.datasource.dbcp2.url"));
        dataSource.setUsername(environment.getProperty("spring.datasource.dbcp2.username"));
        dataSource.setPassword(environment.getProperty("spring.datasource.dbcp2.password"));
        dataSource.setDefaultAutoCommit(Boolean.valueOf(environment.getProperty("spring.datasource.dbcp2.defaultAutoCommit")));
        dataSource.setMinIdle(Integer.parseInt(environment.getProperty("spring.datasource.dbcp2.minIdle")));
        dataSource.setMaxIdle(Integer.parseInt(environment.getProperty("spring.datasource.dbcp2.maxIdle")));
        dataSource.setMaxOpenPreparedStatements(Integer.parseInt(environment.getProperty("spring.datasource.dbcp2.maxOpenPreparedStatements")));
        return dataSource;
    }


    @Bean
    public JdbcTemplate jdbcTemplate() {
        return new JdbcTemplate(dataSource());
    }

    @Bean
    public NamedParameterJdbcTemplate namedParameterJdbcTemplate() {
        return new NamedParameterJdbcTemplate(dataSource());
    }

    @Override
    public void configureTasks(ScheduledTaskRegistrar scheduledTaskRegistrar) {
        ThreadPoolTaskScheduler taskScheduler = new ThreadPoolTaskScheduler();
        taskScheduler.setPoolSize(Integer.parseInt(environment.getProperty("scheduler.pool-size")));
        taskScheduler.initialize();
        scheduledTaskRegistrar.setTaskScheduler(taskScheduler);
    }

    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }


//    @Bean
//    public static PropertySourcesPlaceholderConfigurer properties() throws IOException {
//        logger.info("Loading configuration.");
//
//        PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer = new PropertySourcesPlaceholderConfigurer();
//        YamlPropertiesFactoryBean yamlPropertiesFactoryBean = new YamlPropertiesFactoryBean();
//
//        Resource yamlResource = new ClassPathResource("application.yml");
//
//        logger.info("\tConfiguration loaded from {}", yamlResource.getURL().getPath());
//
//        yamlPropertiesFactoryBean.setResources(yamlResource);
//
//
//        propertySourcesPlaceholderConfigurer.setProperties(yamlPropertiesFactoryBean.getObject());
//        logger.info("Loading configuration. DONE");
//        return propertySourcesPlaceholderConfigurer;
//    }

    @Bean
    public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
        return new PropertySourcesPlaceholderConfigurer();
    }


//    @Override
//    public void onStartup(ServletContext container) throws ServletException {
//// Creates the dispatcher servlet context
//        AnnotationConfigWebApplicationContext servletContext =
//                new AnnotationConfigWebApplicationContext();
//
//// Registers the servlet configuraton with the dispatcher servlet context
//        servletContext.register(AppConfiguration.class);
//
//// Further configures the servlet context
//        ServletRegistration.Dynamic dispatcher =
//                container.addServlet("spring-mvc-dispatcher",
//                        new DispatcherServlet(servletContext));
//        dispatcher.setLoadOnStartup(1);
//        dispatcher.addMapping("/v1/*");
//    }

}