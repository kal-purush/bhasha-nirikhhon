package ru.yandex.practicum.filmorate;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.yandex.practicum.filmorate.controller.FilmController;
import ru.yandex.practicum.filmorate.exception.ValidationException;
import ru.yandex.practicum.filmorate.model.Film;

import java.time.Duration;
import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class FilmControllerTest {

    private FilmController filmController;
    private Film film;

    @BeforeEach
    public void setUp() {

        filmController = new FilmController();

        film = Film.builder()
                .name("Пираты Карибского Моря: Проклятие Чёрной жемчужины")
                .description("Жизнь харизматичного авантюриста, капитана Джека Воробья, " +
                        "полная увлекательных приключений, резко меняется, " +
                        "когда его заклятый враг капитан Барбосса похищает корабль Джека Черную Жемчужину...")
                .releaseDate(LocalDate.of(2003,6,28))
                .duration(Duration.ofHours(2).plus(Duration.ofMinutes(43)))
                .build();

    }

    @Test
    public void shouldCreateFilmWhenAllFieldsAreCorrect() {
        Film pirates = filmController.create(film);
        assertEquals(film, pirates);
        assertEquals(1, filmController.getAll().size(), "Размер списка фильмов должен быть равен 1");
    }

    // при пустом названии должна быть выброшена ошибка валидации
    @Test
    public void shouldNotCreateFilmWhenNameIsEmpty() {
        film.setName("");
        assertThrows(ValidationException.class, () -> filmController.create(film));
        assertEquals(0, filmController.getAll().size(), "Список фильмов должен быть пустым");
    }

    // должна быть выброшена ошибка валидации при длине описания > 200 символов
    @Test
    public void shouldNotCreateFilmWhenDescriptionIsGreaterThan200Symbols() {
        film.setDescription(film.getDescription() + film.getDescription());
        assertThrows(ValidationException.class, () -> filmController.create(film));
        assertEquals(0, filmController.getAll().size(), "Размер списка фильмов должен быть равен 0");
    }

    // должна быть выброшена ошибка валидации при пустом описании фильма
    @Test
    public void shouldNotCreateFilmWhenFilmDescriptionIsEmpty() {
        film.setDescription("");
        assertThrows(ValidationException.class, () -> filmController.create(film));
        assertEquals(0, filmController.getAll().size(), "Размер списка фильмов должен быть равен 0");
    }

    // должна быть выброшена ошибка валидации если дата релиза фильма раньше 28.12.1895
    @Test
    public void shouldNotCreateFilmWhenFilmReleaseDateIsBefore28121895() {
        film.setReleaseDate(LocalDate.of(1895,1,1));
        assertThrows(ValidationException.class, () -> filmController.create(film));
        assertEquals(0, filmController.getAll().size(), "Размер списка фильмов должен быть равен 0");
    }

    // должна быть выброшена ошибка валидации, когда длительность фильма равна нулю
    @Test
    public void shouldNotCreateFilmWhenFilmDurationIsZero() {
        film.setDuration(Duration.ZERO);
        assertThrows(ValidationException.class, () -> filmController.create(film));
        assertEquals(0, filmController.getAll().size(), "Размер списка фильмов должен быть равен 0");
    }

}