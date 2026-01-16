package com.online.cinema;

import com.online.cinema.lib.Injector;
import com.online.cinema.model.Movie;
import com.online.cinema.service.MovieService;

public class Main {
    private static Injector injector = Injector.getInstance("com.online.cinema");

    public static void main(String[] args) {
        Movie movie = new Movie();
        movie.setTitle("Fast and Furious");
        MovieService movieService = (MovieService) injector.getInstance(MovieService.class);
        movieService.add(movie);

        movieService.getAll().forEach(System.out::println);
    }
}