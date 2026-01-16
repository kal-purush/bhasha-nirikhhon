package com.siatsenko.movieland.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class QueryConfig {

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

}