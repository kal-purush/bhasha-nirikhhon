package ru.yandex.practicum.filmorate.validation;

import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import ru.yandex.practicum.filmorate.exceptions.ValidationException;

import java.util.List;

import static ru.yandex.practicum.filmorate.storage.SqlRequests.SQL_GET_USER_FRIEND_LIST_BY_ID;

@Slf4j
public class FriendValidator {
    public static boolean isFriendship(JdbcTemplate jdbcTemplate, long id, long friendId) {
        try {
            List<Long> friendsOfFriendIDs = jdbcTemplate.query(SQL_GET_USER_FRIEND_LIST_BY_ID, (rs, rowNum) -> rs.getLong("FRIEND_ID"), friendId);

            return friendsOfFriendIDs.stream().anyMatch(friendsOfFriend -> friendsOfFriend == id);
        }catch(DuplicateKeyException e) {
            throw new ValidationException("Такая запись уже существует");
        }
    }
}