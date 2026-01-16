package reiner.example.movie.service;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.bson.types.ObjectId;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import reiner.example.movie.model.Movie;
import reiner.example.movie.repository.MovieRepository;

@ContextConfiguration(classes = {MovieService.class})
@ExtendWith(MockitoExtension.class) // 使用 Mockito 的 JUnit 5 扩展
class MovieServiceDiffblueTest {

    @Mock
    private MovieRepository movieRepository; // 直接使用 @Mock，而不是 @MockBean

    @InjectMocks
    private MovieService movieService; // 让 Mockito 自动注入 movieRepository

    /**
     * Method under test: {@link MovieService#allMovies()}
     */
    @Test
    void testAllMovies() {
        List<Movie> movieList = new ArrayList<>();
        when(movieRepository.findAll()).thenReturn(movieList);

        List<Movie> actualAllMoviesResult = movieService.allMovies();

        verify(movieRepository).findAll();
        assertTrue(actualAllMoviesResult.isEmpty());
        assertSame(movieList, actualAllMoviesResult);
    }

    /**
     * Method under test: {@link MovieService#getMovieById(ObjectId)}
     */
    @Test
    void testGetMovieById() {
        Movie movie = new Movie();
        movie.setBackdrops(new ArrayList<>());
        movie.setGenres(new ArrayList<>());
        movie.setId(ObjectId.get());
        movie.setImdbId("42");
        movie.setPoster("Poster");
        movie.setReleaseDate("2020-03-01");
        movie.setReviewIds(new ArrayList<>());
        movie.setTitle("Dr");
        movie.setTrailerLink("Trailer Link");
        Optional<Movie> ofResult = Optional.of(movie);
        when(movieRepository.findById(Mockito.<ObjectId>any())).thenReturn(ofResult);
        Optional<Movie> actualMovieById = movieService.getMovieById(ObjectId.get());
        verify(movieRepository).findById(Mockito.<ObjectId>any());
        assertTrue(actualMovieById.isPresent());
        assertSame(ofResult, actualMovieById);
    }
}