package com.example.auth_service.user;

import com.example.auth_service.controller.UserController;
import com.example.auth_service.dto.UpdateEmailRequest;
import com.example.auth_service.dto.UpdateFirstNameRequest;
import com.example.auth_service.dto.UpdateLastNameRequest;
import com.example.auth_service.dto.UserDto;
import com.example.auth_service.service.UserService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@SpringBootTest
@AutoConfigureMockMvc
class UserControllerTest {

    @Mock
    private UserService userService;

    @Mock
    private Authentication authentication;

    @InjectMocks
    private UserController userController;

    // =======================
    // Тест getUserInfo
    // =======================

    @Test
    @DisplayName("GET /users/info - Успешное получение информации о пользователе")
    void getUserInfo_Success() {
        String username = "testUser";
        UserDto expectedDto = new UserDto(username, "test@example.com", "Test", "User");

        when(authentication.getName()).thenReturn(username);
        when(userService.getUserInfo(username)).thenReturn(expectedDto);

        UserDto result = userController.getUserInfo(authentication);

        assertEquals(expectedDto, result);
        verify(userService, times(1)).getUserInfo(username);
    }

    // =======================
    // Тест updateFirstName
    // =======================

    @Test
    @DisplayName("PUT /users/update/first-name - Успешное обновление имени")
    void updateFirstName_Success() {
        String username = "testUser";
        String newFirstName = "NewName";
        UpdateFirstNameRequest request = new UpdateFirstNameRequest(newFirstName);

        UserDto expectedDto = new UserDto();
        expectedDto.setUsername(username);
        expectedDto.setEmail("test@example.com");
        expectedDto.setFirstName(newFirstName);
        expectedDto.setLastName("User");

        when(authentication.getName()).thenReturn(username);
        when(userService.updateFirstName(username, newFirstName)).thenReturn(expectedDto);

        UserDto result = userController.updateFirstName(request, authentication);

        assertEquals(newFirstName, result.getFirstName());
        verify(userService, times(1)).updateFirstName(username, newFirstName);
    }

    // =======================
    // Тест updateLastName
    // =======================

    @Test
    @DisplayName("PUT /users/update/last-name - Успешное обновление фамилии")
    void updateLastName_Success() {
        String username = "testUser";
        String newLastName = "NewLastName";
        UpdateLastNameRequest request = new UpdateLastNameRequest(newLastName);

        UserDto expectedDto = new UserDto();
        expectedDto.setUsername(username);
        expectedDto.setEmail("test@example.com");
        expectedDto.setFirstName("Test");
        expectedDto.setLastName(newLastName);

        when(authentication.getName()).thenReturn(username);
        when(userService.updateLastName(username, newLastName)).thenReturn(expectedDto);

        UserDto result = userController.updateLastName(request, authentication);

        assertEquals(newLastName, result.getLastName());
        verify(userService, times(1)).updateLastName(username, newLastName);
    }

    // =======================
    // Тест updateEmail
    // =======================

    @Test
    @DisplayName("PUT /users/update/email - Успешное обновление email")
    void updateEmail_Success() {
        String username = "testUser";
        String newEmail = "new@example.com";
        UpdateEmailRequest request = new UpdateEmailRequest(newEmail);

        UserDto expectedDto = new UserDto();
        expectedDto.setUsername(username);
        expectedDto.setEmail(newEmail);
        expectedDto.setFirstName("Test");
        expectedDto.setLastName("User");

        when(authentication.getName()).thenReturn(username);
        when(userService.updateEmail(username, newEmail)).thenReturn(expectedDto);

        UserDto result = userController.updateEmail(request, authentication);

        assertEquals(newEmail, result.getEmail()); // Используем getEmail()
        verify(userService, times(1)).updateEmail(username, newEmail);
    }

    // =======================
    // Тест getAllUsersInfo
    // =======================

    @Test
    @DisplayName("GET /users/info/all - Успешное получение списка всех пользователей (для администратора)")
    void getAllUsersInfo_Success() {
        UserDto user1 = new UserDto("user1", "user1@example.com", "User", "One");
        UserDto user2 = new UserDto("user2", "user2@example.com", "User", "Two");

        when(userService.getAllUsersInfo()).thenReturn(List.of(user1, user2));

        List<UserDto> result = userController.getAllUsersInfo();

        assertEquals(2, result.size());
        verify(userService, times(1)).getAllUsersInfo();
    }

    // =======================
    // Тест getUserById
    // =======================

    @Test
    @DisplayName("GET /users/{id} - Успешное получение пользователя по ID (для администратора)")
    void getUserById_Success() {
        Long userId = 1L;
        UserDto expectedDto = new UserDto("testUser", "test@example.com", "Test", "User");

        when(userService.getUserById(userId)).thenReturn(expectedDto);

        UserDto result = userController.getUserById(userId);

        assertEquals(expectedDto, result);
        verify(userService, times(1)).getUserById(userId);
    }

    // =======================
    // Тест deleteUser
    // =======================

    @Test
    @DisplayName("DELETE /users/{id} - Успешное удаление пользователя (для администратора)")
    void deleteUser_Success() {
        Long userId = 1L;

        ResponseEntity<Void> response = userController.deleteUser(userId);

        assertEquals(HttpStatus.NO_CONTENT, response.getStatusCode());
        verify(userService, times(1)).deleteUser(userId);
    }
}