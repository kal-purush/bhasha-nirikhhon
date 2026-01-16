package ru.yandex.practicum.filmorate.dao.impl;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.filmorate.dao.FriendsUserDB;
import ru.yandex.practicum.filmorate.exeption.EntityNotFoundException;
import ru.yandex.practicum.filmorate.model.User;

import java.time.LocalDate;
import java.util.List;

@Service
public class FriendsUserDBImpl implements FriendsUserDB {
    private final JdbcTemplate jdbcTemplate;

    public FriendsUserDBImpl(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void addFriends(int id, int friendsId) {
        if (userIsExist(id) && userIsExist(friendsId)) {
            String sql = "insert into friends (user_id,friend_id) values (?,?)";
            jdbcTemplate.update(sql, id, friendsId);
        } else {
            throw new EntityNotFoundException("Указан пользователь с несуществующим id");
        }
    }

    @Override
    public void dellFriends(Integer id, Integer friendsId) {
        String sql = "delete from friends where user_id = ? and friend_id = ?";
        jdbcTemplate.update(sql, id, friendsId);
    }

    @Override
    public List<User> getFriends(Integer id) {
        String sql = "select * from users left join friends on users.id = friends.friend_id where friends.user_id = ?";
        return jdbcTemplate.query(sql, userRowMapper(), id);
    }

    @Override
    public List<User> getCommonFriends(Integer id, Integer otherId) {
        String sql = "SELECT u.* FROM users u JOIN friends f1 ON u.id = f1.friend_id JOIN friends f2 ON u.id = f2.friend_id WHERE f1.user_id = ? AND f2.user_id = ?";
        if (userIsExist(id) && userIsExist(otherId)) {
            return jdbcTemplate.query(sql, userRowMapper(), id, otherId);
        } else {
            throw new EntityNotFoundException("Указан пользователь с несуществующим id");
        }
    }

    private RowMapper<User> userRowMapper() {
        return (rs, rowNum) -> new User(rs.getInt("id"), rs.getString("email")
                , rs.getString("login"), rs.getString("name")
                , LocalDate.parse(rs.getString("birthday")));
    }

    private boolean userIsExist(int id) {
        String sql = "select count(*) from users where id = ?";
        int count = jdbcTemplate.queryForObject(sql, Integer.class, id);
        return count > 0;
    }
}