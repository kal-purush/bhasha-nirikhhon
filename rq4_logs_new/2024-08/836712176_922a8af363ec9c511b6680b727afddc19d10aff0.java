package org.example.service;

import org.example.config.AppConfig;
import org.example.domain.dto.UserDTO;
import org.example.domain.model.User;
import org.example.domain.model.UserRole;
import org.example.mapper.UserMapper;
import org.example.repository.UserRepository;
import org.example.service.AuthServiceJdbc;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@SpringJUnitConfig
@ContextConfiguration(classes = { AppConfig.class }) // Use your configuration class here
@WebAppConfiguration
public class AuthServiceJdbcTest {

    @Mock
    private UserRepository userRepository;

    @Mock
    private AuditService auditService;

    @Mock
    private UserMapper userMapper;

    @InjectMocks
    private AuthServiceJdbc authService;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testRegisterSuccess() {
        UserDTO userDTO = new UserDTO(1,"testUser", "password", "test@example.com", 25, UserRole.CUSTOMER);
        User newUser = new User("testUser", "password", "test@example.com", 25, UserRole.CUSTOMER, null);

        when(userRepository.findByUsername("testUser")).thenReturn(null);
        doNothing().when(userRepository).create(newUser);
        doNothing().when(auditService).logAction(anyInt(), anyString());

        boolean result = authService.register(userDTO);

        assertTrue(result);
        verify(userRepository, times(1)).create(newUser);
        verify(auditService, times(1)).logAction(anyInt(), "User registered");
    }

    @Test
    public void testRegisterUserAlreadyExists() {
        UserDTO userDTO = new UserDTO(1,"existingUser", "password", "test@example.com", 25, UserRole.CUSTOMER);

        when(userRepository.findByUsername("existingUser")).thenReturn(new User());

        boolean result = authService.register(userDTO);

        assertFalse(result);
        verify(userRepository, never()).create(any());
        verify(auditService, never()).logAction(anyInt(), anyString());
    }

    @Test
    public void testRegisterAdminSuccess() {
        UserDTO userDTO = new UserDTO(1,"adminUser", "password", "admin@example.com", 30, UserRole.CUSTOMER );
        User newAdmin = new User("adminUser", "password", "admin@example.com", 30, UserRole.ADMIN, null);

        when(userRepository.findByUsername("adminUser")).thenReturn(null);
        doNothing().when(userRepository).create(newAdmin);
        doNothing().when(auditService).logAction(anyInt(), anyString());

        authService.registerAdmin(userDTO);

        verify(userRepository, times(1)).create(newAdmin);
        verify(auditService, times(1)).logAction(anyInt(), "Admin registered");
    }

    @Test
    public void testLoginSuccess() {
        UserDTO userDTO = new UserDTO(1,"loginUser", "password", "login@example.com", 25, UserRole.CUSTOMER);
        User user = new User("loginUser", "password", "login@example.com", 25, UserRole.CUSTOMER, null);

        when(userRepository.findByUsername("loginUser")).thenReturn(user);
        when(userMapper.toDTO(user)).thenReturn(userDTO);
        doNothing().when(auditService).logAction(anyInt(), anyString());

        UserDTO result = authService.login("loginUser", "password");

        assertNotNull(result);
        assertEquals("loginUser", result.username());
        verify(auditService, times(1)).logAction(user.getId(), "User logged in");
    }

    @Test
    public void testLoginFailure() {
        when(userRepository.findByUsername("loginUser")).thenReturn(null);

        UserDTO result = authService.login("loginUser", "wrongPassword");

        assertNull(result);
        verify(auditService, never()).logAction(anyInt(), anyString());
    }
}