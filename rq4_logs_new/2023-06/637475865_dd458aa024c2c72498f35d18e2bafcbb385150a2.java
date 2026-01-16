import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.web.servlet.MockMvc;
import ru.yandex.practicum.filmorate.exception.ValidationException;
import ru.yandex.practicum.filmorate.model.Film;
import ru.yandex.practicum.filmorate.storage.film.FilmStorage;
import ru.yandex.practicum.filmorate.storage.film.InMemoryFilmStorage;

import java.time.LocalDate;
import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class FilmTests {
    private final FilmStorage filmStorage = new InMemoryFilmStorage();

    @Autowired
    private MockMvc mockMvc;
    private final String urlTemplate = "/films";

    @Test
    public void testFilmRightCreation() {
        Film film = new Film(12, "Film1", "Корректная дата релиза",
                LocalDate.of(2222, 12, 2), 22, new HashSet<>());
        assertEquals(film, filmStorage.createFilm(film));
    }

    @Test
    public void testFilmInCorrectDateRelease() {
        Film film = new Film(2, "Film2", "Некорректная дата релиза",
                LocalDate.of(1000, 11, 1), 11, new HashSet<>());
        assertThrows(ValidationException.class, () -> filmStorage.createFilm(film));
    }
}