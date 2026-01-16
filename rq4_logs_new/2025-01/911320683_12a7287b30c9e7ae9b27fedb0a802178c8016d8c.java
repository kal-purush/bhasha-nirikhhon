package com.sofkau.bank.services;

import com.sofkau.bank.entities.User;
import com.sofkau.bank.exceptions.AlreadyExistsException;
import com.sofkau.bank.exceptions.NotFoundException;
import com.sofkau.bank.repositories.UserRepository;
import com.sofkau.bank.services.users.UserServiceImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.crypto.password.PasswordEncoder;

import java.util.Optional;

import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class UserServiceTest {
    @Mock
    private UserRepository userRepository;

    @Mock
    private PasswordEncoder passwordEncoder;

    @InjectMocks
    private UserServiceImpl userService;

    @Test
    void whenCreateUserHasANewUserThenReturnUser() {
        // Arrange
        when(userRepository.existsByEmail(anyString()))
                .thenReturn(false);

        when(userRepository.save(any(User.class)))
                .thenReturn(new User());

        // Act & assert
        assertDoesNotThrow(() -> {
            userService.createUser(User.builder()
                    .email("any@mail.com")
                    .build());
        });

        verify(userRepository, times(1))
                .existsByEmail(eq("any@mail.com"));

        verify(userRepository, times(1))
                .save(any(User.class));
    }

    @Test
    void whenCreateUserHasAlreadyCreatedUserThenThrowException() {
        // Arrange
        when(userRepository.existsByEmail(anyString()))
                .thenReturn(true);

        // Act & assert
        assertThrows(AlreadyExistsException.class, () -> {
            userService.createUser(User.builder()
                    .email("")
                    .build());
        });

        verify(userRepository, times(1))
                .existsByEmail(anyString());
        verify(userRepository, never()).save(any());
    }

    @Test
    void whenUpdateUserHasDataUserGetsUpdatedThenReturnUser() {
        // Arrange
        User userStored = spy(User.builder()
                .email("any@mail.com")
                .build());

        User userForUpdate = User.builder()
                .email("alternative@mail.com")
                .name("John")
                .password("p4sS")
                .build();

        String encryptedPass = "encrypted";

        when(userRepository.findByEmail(anyString()))
                .thenReturn(Optional.of(userStored));

        when(userRepository.existsByEmail(anyString()))
                .thenReturn(false);

        when(passwordEncoder.encode(anyString()))
                .thenReturn(encryptedPass);

        when(userRepository.save(any(User.class)))
                .thenReturn(userStored);

        // Act & assert
        User result = assertDoesNotThrow(() ->
                userService.updateUser(userStored.getEmail(), userForUpdate));

        verify(userStored, times(1))
                .setName(eq(userForUpdate.getName()));

        verify(userStored, times(1))
                .setPassword(eq(encryptedPass));

        verify(userStored, times(1))
                .setEmail(eq(userForUpdate.getEmail()));

        verify(userRepository, times(1))
                .existsByEmail(anyString());

        verify(userRepository, times(1))
                .save(eq(userStored));

        assertEquals(userForUpdate.getEmail(), result.getEmail());
        assertEquals(userForUpdate.getName(), result.getName());
        assertEquals(encryptedPass, result.getPassword());
    }

    @Test
    void whenUpdateUserHasDataUserGetsUpdatedWithEqualEmailThenReturnUser() {
        // Arrange
        User userStored = spy(User.builder()
                .email("any@mail.com")
                .name("user")
                .password("p4sS")
                .build());

        User userForUpdate = User.builder()
                .email(userStored.getEmail())
                .build();

        when(userRepository.findByEmail(anyString()))
                .thenReturn(Optional.of(userStored));

        when(userRepository.save(any(User.class)))
                .thenReturn(userStored);

        // Act & assert
        User result = assertDoesNotThrow(() ->
                userService.updateUser(userStored.getEmail(), userForUpdate));

        verify(userStored, never()).setName(anyString());

        verify(userStored, never()).setPassword(anyString());

        verify(userStored, never())
                .setEmail(anyString());

        verify(userRepository, never())
                .existsByEmail(anyString());

        verify(userRepository, times(1))
                .save(eq(userStored));

        assertEquals(userForUpdate.getEmail(), result.getEmail());
        assertEquals(userStored.getName(), result.getName());
        assertEquals(userStored.getPassword(), result.getPassword());
    }

    @Test
    void whenUpdateUserAndEmailNotFoundThenThrowException() {
        // Arrange
        when(userRepository.findByEmail(anyString()))
                .thenReturn(Optional.empty());

        // Act & assert
        assertThrows(NotFoundException.class, () -> userService.updateUser("", new User()));
    }

    @Test
    void whenUpdateUserAndUpdatedEmailAlreadyExistsThenThrowException() {
        // Arrange
        User userStored = spy(User.builder()
                .email("any@mail.com")
                .build());

        User userForUpdate = User.builder()
                .email("alternative@mail.com")
                .name("John")
                .password("p4sS")
                .build();

        String encryptedPass = "encrypted";

        when(userRepository.findByEmail(anyString()))
                .thenReturn(Optional.of(userStored));

        when(userRepository.existsByEmail(anyString()))
                .thenReturn(true);

        when(passwordEncoder.encode(anyString()))
                .thenReturn(encryptedPass);

        // Act & assert
        assertThrows(AlreadyExistsException.class, () ->
                userService.updateUser(userStored.getEmail(), userForUpdate));

        verify(userStored, times(1))
                .setName(eq(userForUpdate.getName()));

        verify(userStored, times(1))
                .setPassword(eq(encryptedPass));

        verify(userStored, never()).setEmail(anyString());

        verify(userRepository, times(1))
                .existsByEmail(anyString());

        verify(userRepository, never())
                .save(eq(userStored));

        assertNotEquals(userForUpdate.getEmail(), userStored.getEmail());
        assertEquals(userForUpdate.getName(), userStored.getName());
        assertEquals(encryptedPass, userStored.getPassword());
    }

    @Test
    void whenDeleteUserThenDoNothing() {
        // Arrange
        doNothing().when(userRepository)
                .deleteByEmail(anyString());

        assertDoesNotThrow(() -> userService.deleteUserByEmail("any@gmail.com"));

        verify(userRepository, times(1))
                .deleteByEmail(eq("any@gmail.com"));
    }
}