package pl.kozhanov.projectmanagementsystem.service;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import pl.kozhanov.projectmanagementsystem.domain.User;

import java.util.List;

public interface UserService {

    List<User> findAll();

    Page<User> findAllByOrderByUsernameAsc(Pageable pageable);

    User findByUsername(String username);

    List<String> findByUsernameLike(String term);

    String getCurrentLoggedInUsername();

    boolean isAdmin();

    boolean hasProjectAuthorities(String currentLoggedInUser, Integer projectId);

    void addUser(User user);

    String saveUser(Integer userId, String newUsername, String[] roles);

    void deleteUser(User user);

    void changeUserPassword(String username, String password);

    void resetUserPassword(User user);
}