package ru.te3ka.telrostestwork.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.web.multipart.MultipartFile;
import ru.te3ka.telrostestwork.controller.UserController;
import ru.te3ka.telrostestwork.entity.User;
import ru.te3ka.telrostestwork.repository.UserRepository;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.internal.verification.VerificationModeFactory.times;

public class UserControllerTest {
    private final String UPLOAD_PHOTO_DIR = "users_photos/";

    @Mock
    private UserService userService;

    @Mock
    private UserRepository userRepository;

    @InjectMocks
    private UserController userController;

    private User user;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        user = new User(1L,
                "Testov",
                "Test",
                "Testovich",
                LocalDate.of(1990, 1, 1),
                "test@example.tst",
                "7987654321",
                ""
        );
    }

    @Test
    void testCreateUser() {
        when(userService.createUser(any(User.class))).thenReturn(user);

        ResponseEntity<User> response = userController.createUser(user);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals("Testov", response.getBody().getLastName());
        verify(userService, times(1)).createUser(user);
    }

    @Test
    void testGetAllUsers() {
        when(userService.getAllUsers()).thenReturn(Arrays.asList(user));

        ResponseEntity<List<User>> response = userController.getAllUsers();

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals(1, response.getBody().size());
        verify(userService, times(1)).getAllUsers();
    }

    @Test
    void testGetUserById() {
        when(userService.getUserById(1L)).thenReturn(Optional.of(user));

        ResponseEntity<User> response = userController.getUserById(1L);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals("Test", response.getBody().getFirstName());
        verify(userService, times(1)).getUserById(1L);
    }

    @Test
    void testUpdateUser() {
        when(userService.updateUser(1L, user)).thenReturn(user);

        ResponseEntity<User> response = userController.updateUser(1L, user);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals("Testov", response.getBody().getLastName());
        verify(userService, times(1)).updateUser(1L, user);
    }

    @Test
    void testDeleteUser() {
        doNothing().when(userService).deleteUser(1L);

        ResponseEntity<Void> response = userController.deleteUser(1L);

        assertEquals(HttpStatus.NO_CONTENT, response.getStatusCode());
        verify(userService, times(1)).deleteUser(1L);
    }

    @Test
    public void testUpdatePhoto_Success() throws IOException {
        Long userId = 1L;
        User user = new User();
        user.setId(userId);
        user.setPathToPhoto(null);

        MockMultipartFile mockFile = new MockMultipartFile("file",
                "test.jpg",
                "image/jpeg",
                "test image content".getBytes()
        );

        when(userRepository.findById(userId)).thenReturn(Optional.of(user));
        when(userRepository.save(any(User.class))).thenReturn(user);
        userService.updatePhoto(userId, mockFile);
        verify(userRepository, times(1)).save(user);

        Path savedFilePath = Paths.get(UPLOAD_PHOTO_DIR + System.currentTimeMillis() + "_" + mockFile.getOriginalFilename());
        Files.createDirectories(savedFilePath.getParent());
        Files.write(savedFilePath, mockFile.getBytes());
    }
}