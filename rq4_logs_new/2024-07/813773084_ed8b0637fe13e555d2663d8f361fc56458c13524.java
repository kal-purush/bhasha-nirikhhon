package ru.yandex.practicum.filmorate.model.films;

import lombok.Data;

@Data
public class FilmGenre {
    Integer id;
    Integer film_id;
    Integer genre_id;
}