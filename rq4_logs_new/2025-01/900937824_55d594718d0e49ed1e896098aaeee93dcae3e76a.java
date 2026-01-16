package ru.yandex.practicum.filmorate.repository.impl;

import lombok.RequiredArgsConstructor;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.filmorate.model.Film;
import ru.yandex.practicum.filmorate.model.Genre;
import ru.yandex.practicum.filmorate.model.Mpaa;
import ru.yandex.practicum.filmorate.model.User;
import ru.yandex.practicum.filmorate.repository.FilmRepository;
import ru.yandex.practicum.filmorate.repository.mappers.FilmRowMapper;
import ru.yandex.practicum.filmorate.repository.mappers.MpaaRowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

@Component
@RequiredArgsConstructor
public class JdbcFilmRepository implements FilmRepository {

    private final NamedParameterJdbcTemplate jdbc;
    private final FilmRowMapper mapper;
    private final MpaaRowMapper ratingMapper;


    @Override
    public Collection<Film> getAll() {
        String filmQuery = "SELECT * FROM films";
        List<Film> films = jdbc.query(filmQuery, mapper);
        if (films.isEmpty()) {
            return films;
        }
        List<Long> filmIds = films.stream().map(Film::getId).collect(Collectors.toList());

        String genreQuery = "SELECT fg.film_id, g.id AS genre_id, g.name AS genre_name " +
                "FROM film_genres fg " +
                "JOIN genres g ON fg.genre_id = g.id " +
                "WHERE fg.film_id IN (:filmIds)";
        MapSqlParameterSource genresParams = new MapSqlParameterSource();
        genresParams.addValue("filmIds", filmIds);
        List<Map<String, Object>> genreResults = jdbc.queryForList(genreQuery, genresParams);

        Map<Long, List<Genre>> mappedGenres = new HashMap<>();
        for (Map<String, Object> row : genreResults) {
            Long filmId = ((Number) row.get("film_id")).longValue();
            int genreId = (int) row.get("genre_id");
            String genreName = (String) row.get("genre_name");

            mappedGenres.computeIfAbsent(filmId, k -> new ArrayList<>())
                    .add(new Genre(genreId, genreName));
        }

        String mpaaQuery = "SELECT * FROM Mpaa";
        List<Mpaa> ratings = jdbc.query(mpaaQuery, ratingMapper);

        Map<Integer, Mpaa> ratingMap = ratings.stream()
                .collect(Collectors.toMap(Mpaa::getId, rating -> rating));

        films.forEach(film -> {
            film.setGenre(new LinkedHashSet<>(mappedGenres.getOrDefault(film.getId(), List.of())));

            Mpaa filmRating = ratingMap.get(film.getRating().getId());
            film.setRating(filmRating != null ? filmRating : new Mpaa(0, "Unknown"));
        });

        return films;
    }


    @Override
    public Optional<Film> get(Long id) {
        String query = "SELECT f.*, m.name mpa_name " +
                "FROM films f " +
                "INNER JOIN MPAA m ON m.id = f.rating_id " +
                "WHERE f.id = :id";
        try {
            Film film = jdbc.queryForObject(query, new MapSqlParameterSource("id", id), mapper);
            return Optional.of(film);
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    @Override
    public Film save(Film film) {
        String sqlQuery = "INSERT INTO films (name, description, release_date, duration, rating_id) " +
                "VALUES (:name, :description, :releaseDate, :duration, :ratingId)";

        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("name", film.getName())
                .addValue("description", film.getDescription())
                .addValue("releaseDate", film.getReleaseDate())
                .addValue("duration", film.getDuration())
                .addValue("ratingId", film.getRating().getId());

        GeneratedKeyHolder keyHolder = new GeneratedKeyHolder();
        jdbc.update(sqlQuery, params, keyHolder);

        film.setId(Objects.requireNonNull(keyHolder.getKey()).longValue());
        addGenres(film.getId(), film.getGenre());

        return film;
    }

    private void addGenres(Long filmId, Collection<Genre> filmGenres) {
        if (filmGenres == null || filmGenres.isEmpty()) {
            return;
        }

        String sqlQuery = "INSERT INTO film_genres (film_id, genre_id) VALUES (:filmId, :genreId)";
        List<Map<String, Object>> batchValues = new ArrayList<>();

        for (Genre genre : filmGenres) {
            Map<String, Object> params = new HashMap<>();
            params.put("filmId", filmId);
            params.put("genreId", genre.getId());
            batchValues.add(params);
        }

        jdbc.batchUpdate(sqlQuery, batchValues.toArray(new Map[0]));
    }

    @Override
    public Film update(Film film) {
        String sqlQuery = "UPDATE films " +
                "SET name = :name, description = :description, release_date = :releaseDate, duration = :duration, rating_id = :ratingId " +
                "WHERE id = :id";

        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("name", film.getName())
                .addValue("description", film.getDescription())
                .addValue("releaseDate", film.getReleaseDate())
                .addValue("duration", film.getDuration())
                .addValue("ratingId", film.getRating().getId())
                .addValue("id", film.getId());

        jdbc.update(sqlQuery, params);

        removeGenreFilm(film.getId());
        addGenres(film.getId(), film.getGenre());

        return film;
    }

    public void removeGenreFilm(Long id) {
        String sqlQuery = "DELETE FROM film_genres WHERE film_id = :filmId";

        jdbc.update(sqlQuery, new MapSqlParameterSource("filmId", id));
    }

    @Override
    public void addLike(Film film, User user) {
        String query = "INSERT INTO likes (user_id, film_id) VALUES (:userId, :filmId)";

        jdbc.update(query, new MapSqlParameterSource()
                .addValue("userId", user.getId())
                .addValue("filmId", film.getId()));
    }

    @Override
    public void removeLike(Film film, User user) {
        String query = "DELETE FROM likes WHERE user_id = :userId AND film_id = :filmId";

        jdbc.update(query, new MapSqlParameterSource()
                .addValue("userId", user.getId())
                .addValue("filmId", film.getId()));
    }

    @Override
    public List<Film> findBestLiked(Integer count) {
        String query = "SELECT f.*, COUNT(l.user_id) AS like_count " +
                "FROM films f " +
                "LEFT JOIN likes l ON f.id = l.film_id " +
                "GROUP BY f.id " +
                "ORDER BY like_count DESC " +
                "LIMIT :count";

        return jdbc.query(query, new MapSqlParameterSource("count", count), mapper);
    }

    @Override
    public void delete(Long id) {
        removeLikeFilm(id);
        removeGenreFilm(id);

        String sqlQuery = "DELETE FROM films WHERE id = :filmId";

        jdbc.update(sqlQuery, new MapSqlParameterSource("filmId", id));
    }

    public void removeLikeFilm(Long id) {
        String sqlQuery = "DELETE FROM likes WHERE film_id = :filmId";

        jdbc.update(sqlQuery, new MapSqlParameterSource("filmId", id));
    }

    private Film mapRowToFilm(ResultSet resultSet, int rowNum) throws SQLException {
        Film film = new Film();
        film.setId(resultSet.getLong("id"));
        film.setName(resultSet.getString("name"));
        film.setDescription(resultSet.getString("description"));
        film.setReleaseDate(resultSet.getDate("release_date").toLocalDate());
        film.setDuration(resultSet.getInt("duration"));

        Mpaa mpa = new Mpaa();
        mpa.setId(resultSet.getInt("rating_id"));
        mpa.setName(resultSet.getString("mpa_name"));
        film.setRating(mpa);

        return film;
    }
}


