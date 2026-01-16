package ru.iflex.springbootdemo.service.impl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.iflex.springbootdemo.model.User;
import ru.iflex.springbootdemo.repository.UserRepository;
import ru.iflex.springbootdemo.service.UserService;
import java.util.List;

@Slf4j
@Service
public class UserServiceImpl implements UserService {
    private UserRepository userRepository;
    @Autowired
    public UserServiceImpl(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    public User saveUser(String firstName, String lastName){
        User user = mapUser(firstName,lastName);
        log.info("Entity save user{}", user);
        return userRepository.save(user);
    }

    @Override
    public User updateUser(Long id, String firstName, String lastName) {
        User user = mapUser(firstName, lastName);
        user.setId(id);
        return userRepository.save(user);
    }

    @Override
    public User getUserById(Long id) {
        log.info("Entity user by id {}", id);
        return userRepository.findById(id).get();
    }

    @Override
    public User deleteUserById(Long id) {
        userRepository.deleteById(id);
        log.info("Entity delete user by id {}", id);
        return null;
    }

    @Override
    public List<User> getAllUsers() {
        log.info("Entity all users");
        return userRepository.findAll();
    }

    /**
    Отдельный метод, для создания и изменения сущности user
     */
    private User mapUser(String firstName, String lastName) {
        User user = new User();
        user.setFirstName(firstName);
        user.setLastName(lastName);
        return user;
    }

}