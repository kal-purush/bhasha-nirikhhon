package org.wecancodeit.backend.BOService;

import java.util.Optional;

import org.apache.catalina.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.wecancodeit.backend.Repositories.UserRepository;

@Service
public class UserService {

    @Autowired
    private UserRepository userRepository;

    public User registerUser(User user) {

        return userRepository.save(user);
    }

    public User findUserByUsername(String username) {

        return userRepository.findByUsername(username);
    }

    public Optional<User> findUserById(Long id) {

        return userRepository.findById(id);
    }

    public User updateUser(User user) {

        return userRepository.save(user);
    }

    public void deleteUser(Long id) {

        userRepository.deleteById(id);
    }
}