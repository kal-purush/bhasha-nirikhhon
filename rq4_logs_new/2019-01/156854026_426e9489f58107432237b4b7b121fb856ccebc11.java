package com.siatsenko.movieland.service.impl;

import com.siatsenko.movieland.entity.Country;
import com.siatsenko.movieland.entity.Genre;
import com.siatsenko.movieland.entity.Movie;
import com.siatsenko.movieland.entity.Review;
import com.siatsenko.movieland.service.CountryService;
import com.siatsenko.movieland.service.EnrichmentService;
import com.siatsenko.movieland.service.GenreService;
import com.siatsenko.movieland.service.ReviewService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

@Service
public class ParallelEnrichmentService implements EnrichmentService {
    private GenreService genreService;
    private CountryService countryService;
    private ReviewService reviewService;

    private static final ExecutorService executorService = Executors.newCachedThreadPool();

    @Override
    public Movie enrich(Movie movie) {
//        genreService.enrich(movie);
//        countryService.enrich(movie);
//        reviewService.enrich(movie);
//        List<Genre> genres = new ArrayList<>(genreService.getByMovieId(movie.getId()));
//        List<Country> countries = new ArrayList<>(countryService.getByMovieId(movie.getId()));
//        List<Review> reviews = new ArrayList<>(reviewService.getByMovieId(movie.getId()));

        int movieId = movie.getId();
        List<Callable<List<?>>> tasks = Arrays.asList(
                () -> {return new ArrayList<>(genreService.getByMovieId(movieId));},
                () -> {return new ArrayList<>(countryService.getByMovieId(movieId));},
                () -> {return new ArrayList<>(reviewService.getByMovieId(movieId));}
        );


        try {
executorService.invokeAll(tasks,5000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
        e.printStackTrace();
    }


        return movie;
    }

    @Autowired
    public void setGenreService(GenreService genreService) {
        this.genreService = genreService;
    }

    @Autowired
    public void setCountryService(CountryService countryService) {
        this.countryService = countryService;
    }

    @Autowired
    public void setReviewService(ReviewService reviewService) {
        this.reviewService = reviewService;
    }
}