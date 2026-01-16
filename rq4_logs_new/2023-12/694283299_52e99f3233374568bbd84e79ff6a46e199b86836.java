package com.myproject.bikereviewapp.service.impl;

import com.myproject.bikereviewapp.entity.Role;
import com.myproject.bikereviewapp.entity.User;
import com.myproject.bikereviewapp.exceptionhandler.exception.EntityNotFoundException;
import com.myproject.bikereviewapp.exceptionhandler.exception.UserDuplicationException;
import com.myproject.bikereviewapp.repository.UserRepository;
import com.myproject.bikereviewapp.service.abstraction.UserService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.security.crypto.password.PasswordEncoder;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@SpringBootTest(classes = UserServiceImpl.class)
class UserServiceImplTest {

    @MockBean
    UserRepository userRepository;

    @MockBean
    PasswordEncoder passwordEncoder;

    @Autowired
    UserService userService;

    @Test
    void getById_shouldReturnUser_whenUserWasFound() {
        Long id = 8L;
        User user = new User(id, "someUsername", "somePassword", true, Role.CLIENT, "somePublicName");

        when(userRepository.findById(id)).thenReturn(Optional.of(user));

        assertEquals(user, userService.getById(id));

        verify(userRepository).findById(id);
    }

    @Test
    void getById_shouldThrowException_whenUserWasNotFound() {
        Long id = 8L;

        when(userRepository.findById(id)).thenReturn(Optional.empty());

        assertThrows(EntityNotFoundException.class, () -> userService.getById(id));

        verify(userRepository).findById(id);
    }

    @Test
    void getByUsername_shouldReturnUser_whenUserWasFound() {
        Long id = 8L;
        String username = "someUsername";
        User user = new User(id, username, "somePassword", true, Role.CLIENT, "somePublicName");

        when(userRepository.findByUsername(username)).thenReturn(Optional.of(user));

        assertEquals(user, userService.getByUsername(username));

        verify(userRepository).findByUsername(username);
    }

    @Test
    void getByUsername_shouldThrowException_whenUserWasNotFound() {
        String username = "someUsername";

        when(userRepository.findByUsername(username)).thenReturn(Optional.empty());

        assertThrows(EntityNotFoundException.class, () -> userService.getByUsername(username));

        verify(userRepository).findByUsername(username);
    }

    @Test
    void create_shouldSaveUser_ifUserIsUnique() {
        String username = "someUsername";
        User user = new User(8L, username, "somePassword", true, Role.CLIENT, "somePublicName");

        when(userRepository.existsUserByUsername(username)).thenReturn(false);

        userService.create(user);

        verify(userRepository).existsUserByUsername(username);
        verify(userRepository).save(any(User.class));
    }

    @Test
    void create_shouldSaveAndReturnUserWithEncodedPassword_ifUserIsUnique() {
        String username = "someUsername";

        String rawPassword = "rawPassword";
        String encodedPassword = "encodedPassword";

        User user = new User(8L, username, rawPassword, true, Role.CLIENT, "somePublicName");
        User expectedUser = new User(user.getId(), user.getUsername(), encodedPassword, user.isEnabled(), user.getRole(), user.getPublicName());

        when(userRepository.existsUserByUsername(username)).thenReturn(false);
        when(passwordEncoder.encode(rawPassword)).thenReturn(encodedPassword);
        when(userRepository.save(expectedUser)).thenReturn(expectedUser);

        assertEquals(expectedUser, userService.create(user));

        verify(userRepository).existsUserByUsername(username);
        verify(passwordEncoder).encode(rawPassword);
        verify(userRepository).save(expectedUser);
    }

    @Test
    void create_shouldThrowException_ifUserIsDuplicated() {
        String username = "someUsername";
        User user = new User(8L, username, "somePassword", true, Role.CLIENT, "somePublicName");

        when(userRepository.existsUserByUsername(username)).thenReturn(true);

        assertThrows(UserDuplicationException.class, () -> userService.create(user));

        verify(userRepository).existsUserByUsername(username);
        verify(userRepository, never()).save(any());
    }

}