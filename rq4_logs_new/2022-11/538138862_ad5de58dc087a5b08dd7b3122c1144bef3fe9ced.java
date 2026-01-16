package ru.yandex.practicum.filmorate.validation;

import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import ru.yandex.practicum.filmorate.exceptions.ValidationException;
import ru.yandex.practicum.filmorate.model.Film;
import ru.yandex.practicum.filmorate.storage.SqlRequests;

import java.time.LocalDate;
import java.util.Optional;

@Slf4j
public class FilmValidator {
    private static final int DESCRIPTION_MAX_LENGTH = 200;
    private static final int GENRE_COUNT = 6;
    private static final LocalDate FILM_EARLIEST_RELEASE_DATE = LocalDate.of(1895, 12, 28);

    // Валидация данных для фильма
    public static Film validateFilmInfo(Film film) {
        if (
                film != null && film.getMpa().getId() != null
                        && film.getDescription().length() < DESCRIPTION_MAX_LENGTH
                        && film.getReleaseDate().isAfter(FILM_EARLIEST_RELEASE_DATE)
                        && film.getDuration() > 0
                        && (film.getMpa().getId() < GENRE_COUNT || film.getMpa().getId() > 1)
        ) {
            log.debug("Валидация фильма прошла успешно!");
            return film;
        }

        log.debug("Фильм не прошел валидацию {}", film);

        throw new ValidationException("Для фильма указаны неверные данные!");
    }



    public static Boolean validateFilmExistsInDB(JdbcTemplate jdbcTemplate, Film film) {
        Optional<Boolean> filmIsExist = Optional.ofNullable(jdbcTemplate.queryForObject(SqlRequests.SQL_FIND_FILM_ID_BOOLEAN, Boolean.class, film.getId()));
        if (filmIsExist.isPresent()) {
            if (filmIsExist.get()){
                log.debug("Фильм c ID = {} найден в БД", film.getId());

                return true;
            } else {
                log.debug("Фильм c ID = {} НЕ найден в БД", film.getId());

                return false;
            }

        } else {
            log.debug("Фильм НЕ прошел валидацию: {}", film);

            throw new ValidationException("Ошибка при обращении к БД!");
        }
    }
}