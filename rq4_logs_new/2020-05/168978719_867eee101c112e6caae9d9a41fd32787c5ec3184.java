package azmain.github.io.controller.movie;

import azmain.github.io.domain.movie.Movie;
import azmain.github.io.service.movie.MovieService;
import org.springframework.context.annotation.Bean;
import org.springframework.data.domain.Pageable;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;

@RestController
@RequestMapping(path = "movies", produces = MediaType.APPLICATION_JSON_VALUE)
public class MovieController {

    private final MovieService movieService;

    public MovieController(MovieService movieService) {
        this.movieService = movieService;
    }

    @GetMapping
    public ResponseEntity<?> getMovies(
            Pageable pageable,
            @RequestParam(value = "asPage", defaultValue = "false")final boolean asPage){
        if(asPage)
            return ResponseEntity.ok(movieService.getAllMovies(pageable));
        else
            return ResponseEntity.ok(movieService.getAllMovies());
    }

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> createMovie(@RequestBody @Valid Movie movie){
        return ResponseEntity.ok(movieService.saveMovie(movie));
    }

    @PutMapping(path = "{id}", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> createMovie(
            @PathVariable(name = "id") final long movieId,
            @RequestBody @Valid Movie movie){
        movieService.updateMovie(movieId, movie);
        return ResponseEntity.ok().build();
    }

    @GetMapping(path = "{id}")
    public ResponseEntity<?> getMovie(
            @PathVariable(name = "id") final long movieId){

        return ResponseEntity.ok(movieService.getMovie(movieId));
    }

    @DeleteMapping(path = "{id}")
    public ResponseEntity<?> deleteMovie(
            @PathVariable(name = "id") final long movieId){
        movieService.deleteMovie(movieId);
        return ResponseEntity.ok().build();
    }

}