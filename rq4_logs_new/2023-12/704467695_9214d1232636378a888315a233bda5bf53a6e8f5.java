package ru.yandex.practicum.filmorate.dao.impl;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.filmorate.dao.FilmStorage;
import ru.yandex.practicum.filmorate.dao.RecommendationStorage;
import ru.yandex.practicum.filmorate.model.Film;
import ru.yandex.practicum.filmorate.model.Likes;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class RecommendationDaoImpl implements RecommendationStorage {

    private final JdbcTemplate jdbcTemplate;
    private final FilmStorage filmStorage;

    public RecommendationDaoImpl(JdbcTemplate jdbcTemplate, @Qualifier("filmDaoImpl") FilmStorage filmStorage) {
        this.jdbcTemplate = jdbcTemplate;
        this.filmStorage = filmStorage;
    }

    @Override
    public List<Film> getRecommendedFilms(Long id) {

        List<Likes> allLikes = jdbcTemplate.query("SELECT * FROM Likes", this::rsToLike);
        // получаем список id всех пользователей и список id фильмов, который они пролайкали.
        Map<Long, List<Long>> usersAndLikedFilmsIds = makeMapOfUsersAndLikes(allLikes);
        // получаем список рекомендаций для пользователя
        List<Long> listOfRecommendedFilmsIds = makeRecommendationsList(usersAndLikedFilmsIds, id);

        if (listOfRecommendedFilmsIds.size() < 1) {
            return Collections.emptyList();
        }
        // получаем список рекомендованных фильмов
        return listOfRecommendedFilmsIds.stream()
                .map(filmStorage::getFilmsId)
                .collect(Collectors.toList());
    }

    private List<Long> makeRecommendationsList(Map<Long, List<Long>> usersAndLikedFilmsIds, Long id) {
        // создаем и заполняем список id пользователей и количества совпадений по лайкам с нашим пользователем.
        List<Long> userLikedFilmsIds = usersAndLikedFilmsIds.get(id);
        Map<Long, Long> idsAndMatchesCount = new HashMap<>();

        for (Long otherUserId : usersAndLikedFilmsIds.keySet()) {
            if (otherUserId == id) {
                continue;
            }
            List<Long> userLikes = new ArrayList<>(userLikedFilmsIds);
            userLikes.retainAll(usersAndLikedFilmsIds.get(otherUserId));
            if (userLikes.isEmpty()) {
                continue;
            }
            long commonLikesCount = userLikes.size();
            idsAndMatchesCount.put(otherUserId, commonLikesCount);
        }

        if (idsAndMatchesCount.isEmpty()) {
            return Collections.emptyList();
        }
        // Найти топ 10 пользователей с максимальным количеством пересечения по лайкам.
        List<Long> topTenMatchesUsersIds = idsAndMatchesCount.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())).limit(10)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());

        // Определить id фильмов, которые наш пользователь не лайкал.
        return topTenMatchesUsersIds.stream()
                .map(usersAndLikedFilmsIds::get)
                .flatMap(List::stream)
                .filter(filmId -> !userLikedFilmsIds.contains(filmId))
                .distinct()
                .sorted()
                .collect(Collectors.toList());
    }

    private Map<Long, List<Long>> makeMapOfUsersAndLikes(List<Likes> allLikes) {
        return allLikes.stream()
                .collect(Collectors.toMap(
                        Likes::getUserId,
                        like -> List.of(like.getFilmId()),
                        this::concatenate
                ));
    }

    private <T> List<T> concatenate(List<T> first, List<T> second) {
        List<T> result = new ArrayList<>(first);
        result.addAll(second);
        return result;
    }

    private Likes rsToLike(ResultSet rs, int rowNum) throws SQLException {
        Long userId = rs.getLong("user_id");
        Long filmId = rs.getLong("film_id");
        return Likes.builder()
                .userId(userId)
                .filmId(filmId)
                .build();
    }
}