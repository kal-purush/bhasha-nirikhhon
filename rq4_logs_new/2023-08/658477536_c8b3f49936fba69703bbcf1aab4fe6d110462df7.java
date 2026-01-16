package by.it.medved.controllers;

import by.it.medved.dto.MovieRequest;
import by.it.medved.dto.MovieResponse;
import by.it.medved.dto.UpdateMovieRequest;
import by.it.medved.services.MovieService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Validated
@RestController
@RequestMapping("api/v1")
@RequiredArgsConstructor
public class MovieController {

    private final MovieService movieService;

    @PostMapping("movieSave")
    public MovieResponse addMovie(@Valid @RequestBody MovieRequest movieRequest) {
        return movieService.addMovie(movieRequest);
    }

    @GetMapping("/movieById/{id}")
    public MovieResponse getMovieById(@PathVariable Long id) {
        return movieService.getMovieById(id);
    }

    @GetMapping("/movies")
    public List<MovieResponse> getMovies() {
        return movieService.getMovies();
    }

    @GetMapping("/movieUpdate")
    public MovieResponse updateMovie(@RequestBody UpdateMovieRequest updateMovieRequest) {
        return movieService.updateMovie(
                updateMovieRequest.getId(),
                updateMovieRequest.getShowDateTime(),
                updateMovieRequest.getPrice()
        );
    }

    @GetMapping("/movieDelete/{id}")
    public void deleteMovie(@PathVariable Long id) {
        movieService.deleteMovie(id);
    }
}