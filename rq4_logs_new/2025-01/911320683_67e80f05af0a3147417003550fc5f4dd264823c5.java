package com.sofkau.bank.services.users;

import org.springframework.data.domain.Example;
import org.springframework.data.domain.ExampleMatcher;
import org.springframework.stereotype.Service;

import com.sofkau.bank.entities.User;
import com.sofkau.bank.exceptions.AlreadyExistsException;
import com.sofkau.bank.exceptions.NotFoundException;
import com.sofkau.bank.repositories.UserRepository;

@Service
public class UserServiceImpl implements UserService {
    private final UserRepository userRepository;

    public UserServiceImpl(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    @Override
    public User createUser(User user) {
        if (checkIfUserExists(user))
            throw new AlreadyExistsException();

        return userRepository.save(user);
    }

    @Override
    public User findUserByEmail(String email) {
        return userRepository.findByEmail(email)
                .orElseThrow(NotFoundException::new);
    }

    private boolean checkIfUserExists(User user) {
        ExampleMatcher validator = ExampleMatcher.matchingAny()
                .withMatcher("id", match -> match.exact())
                .withMatcher("email", match -> match.exact())
                .withIgnoreNullValues();

        User sample = User.builder()
                .email(user.getEmail())
                .build();

        Example<User> example = Example.of(sample, validator);

        return userRepository.exists(example);
    }
}