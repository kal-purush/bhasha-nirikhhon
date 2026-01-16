package ru.yandex.practicum.filmorate.service;

import org.springframework.stereotype.Service;
import ru.yandex.practicum.filmorate.exception.NotFoundException;
import ru.yandex.practicum.filmorate.model.User;
import ru.yandex.practicum.filmorate.storage.UserStorage;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

@Service
public class UserService {
    public final UserStorage userStorage;

    public UserService(UserStorage userStorage) {
        this.userStorage = userStorage;
    }

    public void makeFriends(User user1, User user2) {
        userExistsCheck(user1);
        userExistsCheck(user2);
        user1.addFriend(user2);
        user2.addFriend(user1);
    }

    public void removeFriends(User user1, User user2) {
        user1.removeFriend(user2);
        user2.removeFriend(user1);
    }

    public Collection<Long> commonFriends(User user1, User user2) {
        Set<Long> commonFriends = new HashSet<>(user1.getFriends());
        commonFriends.retainAll(user2.getFriends());
        return commonFriends;
    }

    private void userExistsCheck(User user) {
        if (userStorage.findUser(user.getId()).isEmpty()) {
            throw new NotFoundException(String.format("User with id %d not found", user.getId()));
        }
    }
}