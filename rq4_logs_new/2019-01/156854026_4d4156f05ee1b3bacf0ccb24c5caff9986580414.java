package com.siatsenko.movieland.web.controller;

import com.siatsenko.movieland.entity.common.*;
import com.siatsenko.movieland.entity.request.RequestParameters;
import com.siatsenko.movieland.service.MovieService;
import com.siatsenko.movieland.service.RequestParamsService;
import com.siatsenko.movieland.service.impl.DefaultMovieRequestParamsService;
import com.siatsenko.movieland.web.controller.util.DtoConverter;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

import java.util.*;

import static org.hamcrest.collection.IsCollectionWithSize.hasSize;

import static org.hamcrest.CoreMatchers.is;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;


@RunWith(SpringRunner.class)
@WebMvcTest
public class MovieControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private MovieService movieService;

    @Autowired
    RequestParamsService requestParamsService;

    @Configuration
    public static class TestContext {
        @Bean
        public MovieService movieService() {
            return Mockito.mock(MovieService.class);
        }

        @Bean
        public MovieController movieController() {
            return new MovieController();
        }

        @Bean
        public DtoConverter dtoConverter() {
            return new DtoConverter();
        }

        @Bean
        public RequestParamsService requestParamsService() {
            return new DefaultMovieRequestParamsService();
        }
    }

    private List<Movie> movies = new ArrayList<Movie>();

    @Before
    public void beforeTest() {

        Movie movieOne = new Movie();
        movieOne.setId(2);
        movieOne.setNameNative("The Green Mile");
        movieOne.setNameRussian("Зеленая миля");
        movieOne.setYearOfRelease("1999");
        movieOne.setRating(8.9);
        movieOne.setPrice(134.67);
        movieOne.setDescription("Обвиненный в страшном преступлении, Джон Коффи оказывается в блоке смертников тюрьмы «Холодная гора». Вновь прибывший обладал поразительным ростом и был пугающе спокоен, что, впрочем, никак не влияло на отношение к нему начальника блока Пола Эджкомба, привыкшего исполнять приговор.");
        movieOne.setPicturePath("https://images-na.ssl-images-amazon.com/images/M/MV5BMTUxMzQyNjA5MF5BMl5BanBnXkFtZTYwOTU2NTY3._V1._SY209_CR0,0,140,209_.jpg");
        movieOne.setCountries(Arrays.asList(new Country(1, "США")));
        movieOne.setGenres(Arrays.asList(new Genre(3, "фэнтези"), new Genre(1, "драма"), new Genre(2, "криминал"), new Genre(4, "детектив")));
        movieOne.setReviews(Arrays.asList(new Review(2, new User(1, "Рональд Рейнольдс", "ronald.reynolds66@example.com", Role.ADMIN), "Перестал удивляться тому, что этот фильм занимает сплошь первые места во всевозможных кино рейтингах. Особенно я люблю когда при экранизации литературного произведение из него в силу специфики кинематографа исчезает ирония и появляется некий сверхреализм, обязанный удерживать зрителя у экрана каждую отдельно взятую секунду. ")));
        movies.add(movieOne);

        Movie movieTwo = new Movie();
        movieTwo.setId(4);
        movieTwo.setNameNative("Dances with Wolves");
        movieTwo.setNameRussian("Танцующий с волками");
        movieTwo.setYearOfRelease("1990");
        movieTwo.setRating(8.00);
        movieTwo.setPrice(120.55);
        movieTwo.setDescription("Действие фильма происходит в США во времена Гражданской войны. Лейтенант американской армии Джон Данбар после ранения в бою просит перевести его на новое место службы ближе к западной границе США.");
        movieTwo.setPicturePath("https://images-na.ssl-images-amazon.com/images/M/MV5BMTY3OTI5NDczN15BMl5BanBnXkFtZTcwNDA0NDY3Mw@@._V1._SX140_CR0,0,140,209_.jpg");
        movieTwo.setCountries(Arrays.asList(new Country(1, "США"), new Country(3, "Великобритания")));
        movieTwo.setGenres(Arrays.asList(new Genre(1, "драма"), new Genre(11, "приключения"), new Genre(15, "вестерн")));
        movieTwo.setReviews(Arrays.asList(new Review(2, new User(7, "Амелия Кэннеди", "amelia.kennedy58@example.com", Role.USER), "Нетленный шедевр мирового кинематографа, который можно пересматривать десятки раз и получать все такой — же, извините за выражение, кайф от просмотра. Минусы у фильма, конечно, есть, но черт возьми. Их просто не хочется замечать! Ты настолько поглощен просмотром фильма, что просто не хочется придираться к разным мелочам. ")));
        movies.add(movieTwo);
    }

    @Test
    public void GetById() throws Exception {
        when(movieService.getById(2)).thenReturn(movies.get(0));

        mockMvc.perform(get("/movie/{movieId}", 2))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(jsonPath("id", is(2)))
                .andExpect(jsonPath("nameNative", is("The Green Mile")))
                .andExpect(jsonPath("nameRussian", is("Зеленая миля")))
                .andExpect(jsonPath("yearOfRelease", is("1999")))
                .andExpect(jsonPath("rating", is(8.9)))
                .andExpect(jsonPath("price", is(134.67)))
                .andExpect(jsonPath("description", is("Обвиненный в страшном преступлении, Джон Коффи оказывается в блоке смертников тюрьмы «Холодная гора». Вновь прибывший обладал поразительным ростом и был пугающе спокоен, что, впрочем, никак не влияло на отношение к нему начальника блока Пола Эджкомба, привыкшего исполнять приговор.")))
                .andExpect(jsonPath("picturePath", is("https://images-na.ssl-images-amazon.com/images/M/MV5BMTUxMzQyNjA5MF5BMl5BanBnXkFtZTYwOTU2NTY3._V1._SY209_CR0,0,140,209_.jpg")))

                .andExpect(jsonPath("countries", hasSize(1)))
                .andExpect(jsonPath("countries[0].id", is(1)))
                .andExpect(jsonPath("countries[0].name", is("США")))

                .andExpect(jsonPath("genres", hasSize(4)))
                .andExpect(jsonPath("genres[0].id", is(3)))
                .andExpect(jsonPath("genres[0].name", is("фэнтези")))
                .andExpect(jsonPath("genres[1].id", is(1)))
                .andExpect(jsonPath("genres[1].name", is("драма")))
                .andExpect(jsonPath("genres[2].id", is(2)))
                .andExpect(jsonPath("genres[2].name", is("криминал")))
                .andExpect(jsonPath("genres[3].id", is(4)))
                .andExpect(jsonPath("genres[3].name", is("детектив")))

                .andExpect(jsonPath("reviews", hasSize(1)))
                .andExpect(jsonPath("reviews[0].id", is(2)))
                .andExpect(jsonPath("reviews[0].text", is("Перестал удивляться тому, что этот фильм занимает сплошь первые места во всевозможных кино рейтингах. Особенно я люблю когда при экранизации литературного произведение из него в силу специфики кинематографа исчезает ирония и появляется некий сверхреализм, обязанный удерживать зрителя у экрана каждую отдельно взятую секунду. ")))
                .andExpect(jsonPath("reviews[0].user.id", is(1)))
                .andExpect(jsonPath("reviews[0].user.name", is("Рональд Рейнольдс")))
        ;
    }


    @Test
    public void getAll() throws Exception {
        RequestParameters requestParameters = requestParamsService.setSortings(new HashMap<String, String>());
        when(movieService.getAll(requestParameters)).thenReturn(movies);

        mockMvc.perform(get("/movie"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(jsonPath("$", hasSize(2)))
                .andExpect(jsonPath("$[0].id", is(2)))
                .andExpect(jsonPath("$[0].nameNative", is("The Green Mile")))
                .andExpect(jsonPath("$[0].nameRussian", is("Зеленая миля")))
                .andExpect(jsonPath("$[0].yearOfRelease", is("1999")))
                .andExpect(jsonPath("$[0].rating", is(8.9)))
                .andExpect(jsonPath("$[0].price", is(134.67)))
                .andExpect(jsonPath("$[0].picturePath", is("https://images-na.ssl-images-amazon.com/images/M/MV5BMTUxMzQyNjA5MF5BMl5BanBnXkFtZTYwOTU2NTY3._V1._SY209_CR0,0,140,209_.jpg")))
        ;
    }


    @Test
    public void getByGenreId() throws Exception {
        RequestParameters requestParameters = requestParamsService.setSortings(new HashMap<String, String>());
        when(movieService.getByGenreId(1, requestParameters)).thenReturn(movies);

        mockMvc.perform(get("/movie/genre/{genreId}", 1, requestParameters))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(jsonPath("$", hasSize(2)))
                .andExpect(jsonPath("$[0].id", is(2)))
                .andExpect(jsonPath("$[0].nameNative", is("The Green Mile")))
                .andExpect(jsonPath("$[0].nameRussian", is("Зеленая миля")))
                .andExpect(jsonPath("$[0].yearOfRelease", is("1999")))
                .andExpect(jsonPath("$[0].rating", is(8.9)))
                .andExpect(jsonPath("$[0].price", is(134.67)))
                .andExpect(jsonPath("$[0].picturePath", is("https://images-na.ssl-images-amazon.com/images/M/MV5BMTUxMzQyNjA5MF5BMl5BanBnXkFtZTYwOTU2NTY3._V1._SY209_CR0,0,140,209_.jpg")))
        ;

    }

}