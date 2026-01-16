package com.danigu.web.user;

import com.danigu.web.user.dto.CreateUserDto;
import com.danigu.web.user.validator.CreateUserDtoValidator;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import javax.validation.Valid;

/**
 * @author dani
 */
@Service
public class UserService {

    @Inject private UserRepository repository;
    @Inject private PasswordEncoder encoder;

    /**
     * Assume the {@link CreateUserDto} is valid, only source is the {@link UserController}.
     * {@link CreateUserDtoValidator} could've been reused here, i've opened a suggestion for this:
     * - https://jira.spring.io/browse/SPR-15314
     * @param dto
     * @return
     */
    public User create(CreateUserDto dto) {
        User newUser = new User();

        newUser.setUsername(dto.getUsername());
        newUser.setPassword(encoder.encode(dto.getPassword()));

        return repository.saveAndFlush(newUser);
    }
}