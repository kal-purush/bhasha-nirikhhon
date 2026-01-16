package com.example.auth_service.controller.user;

import com.example.auth_service.exception.UserNotFoundException;
import com.example.auth_service.service.user.UserDeleteService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.Mockito.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.user;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class UserDeleteControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Mock
    private UserDeleteService userDeleteService;

    @InjectMocks
    private UserDeleteController userDeleteController;

    private Long userId;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        userId = 1L;
    }

    @Test
    @DisplayName("Позитивный тест: успешное удаление пользователя")
    @WithMockUser(roles = "ADMIN")
    void testDeleteUserSuccess() {
        doNothing().when(userDeleteService).deleteUser(userId);

        ResponseEntity<Void> response = userDeleteController.deleteUser(userId);

        assertEquals(HttpStatus.NO_CONTENT, response.getStatusCode());
        verify(userDeleteService, times(1)).deleteUser(userId);
    }

    @Test
    @DisplayName("Негативный тест: попытка удаления пользователя без прав администратора")
    void testDeleteUserForbidden() throws Exception {
        // Пользователь с ролью "USER"
        Long userId = 1L;

        // Выполнение запроса на удаление
        mockMvc.perform(delete("/users/{userId}", userId)
                        .with(user("user").roles("USER")))  // Устанавливаем роль "USER"
                .andExpect(status().isForbidden())  // Ожидаем статус 403
                .andExpect(content().string(""));  // Тело ответа должно быть пустым

        // Проверка, что метод удаления не был вызван
        verify(userDeleteService, never()).deleteUser(userId);
    }



    @Test
    @DisplayName("Негативный тест: пользователь не найден при удалении")
    @WithMockUser(roles = "ADMIN")
    void testDeleteUserNotFound() {
        doThrow(new UserNotFoundException("Пользователь не найден")).when(userDeleteService).deleteUser(userId);

        UserNotFoundException exception = assertThrows(UserNotFoundException.class, () -> userDeleteController.deleteUser(userId));

        assertEquals("Пользователь не найден", exception.getMessage());
        verify(userDeleteService, times(1)).deleteUser(userId);
    }
}