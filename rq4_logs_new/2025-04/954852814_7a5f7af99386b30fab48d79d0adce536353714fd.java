package com.example.auth_service.controller.user;

import com.example.auth_service.service.user.UserDeleteService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

/**
 * Контроллер для удаления пользователей.
 */
@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/users")
public class UserDeleteController {

    private final UserDeleteService userDeleteService;

    /**
     * Удаляет пользователя (только для администратора).
     */
    @DeleteMapping("/{id}")
    @PreAuthorize("hasRole('ADMIN')")
    public void deleteUser(@PathVariable Long id) {
        log.warn("Администратор удаляет пользователя с ID: {}", id);
        userDeleteService.deleteUser(id);
    }
}