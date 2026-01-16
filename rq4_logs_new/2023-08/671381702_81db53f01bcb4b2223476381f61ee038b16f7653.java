package com.bookstore.persistence.service;

import com.bookstore.persistence.dto.UserDTO;
import com.bookstore.persistence.model.User;
import com.bookstore.persistence.repository.UserDAO;
import com.bookstore.persistence.converter.UserConverter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class UserServiceTest {

    @Mock
    private UserDAO userDAO;

    @InjectMocks
    private UserService userService;

    @SuppressWarnings("deprecation")
	@BeforeEach
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetAllUsersValidDataReturnsAllUsers() {
        List<User> users = new ArrayList<>();
        users.add(new User(1L, "John", "Doe", "john@example.com"));
        users.add(new User(2L, "Jane", "Smith", "jane@example.com"));
        when(userDAO.findAll()).thenReturn(users);

        List<UserDTO> result = userService.getAllUsers();
        assertNotNull(result);
        assertEquals(2, result.size());
    }

    @Test
    public void testGetUserByIdValidIdReturnsUserDTO() {
        Long userId = 1L;
        User user = new User(userId, "John", "Doe", "john@example.com");
        when(userDAO.findById(userId)).thenReturn(Optional.of(user));

        UserDTO result = userService.getUserById(userId);
        assertNotNull(result);
        assertEquals(userId, result.getId());
    }

    @Test
    public void testGetUserByIdInvalidIdReturnsNull() {
        Long userId = 1L;
        when(userDAO.findById(userId)).thenReturn(Optional.empty());

        UserDTO result = userService.getUserById(userId);
        assertNull(result);
    }

    @Test
    public void testCreateUserValidUserDTOCreatesNewUser() {
        UserDTO userDTO = new UserDTO(null, "John", "Doe", "john@example.com");
        when(userDAO.save(any(User.class))).thenReturn(new User(1L, "John", "Doe", "john@example.com"));

        UserDTO result = userService.createUser(userDTO);
        assertNotNull(result);
        assertNotNull(result.getId());
        assertEquals("John", result.getFirstName());
        assertEquals("Doe", result.getLastName());
        assertEquals("john@example.com", result.getEmail());
    }

    @Test
    public void testUpdateUserValidUserDTOUpdatesExistingUser() {
        Long userId = 1L;
        User existingUser = new User(userId, "John", "Doe", "john@example.com");
        when(userDAO.findById(userId)).thenReturn(Optional.of(existingUser));

        UserDTO userDTO = new UserDTO(userId, "Updated", "User", "updated@example.com");
        User updatedUser = UserConverter.convertToEntity(userDTO);
        when(userDAO.save(any(User.class))).thenReturn(updatedUser);

        UserDTO result = userService.updateUser(userDTO);
        assertNotNull(result);
        assertEquals(userId, result.getId());
        assertEquals("Updated", result.getFirstName());
        assertEquals("User", result.getLastName());
        assertEquals("updated@example.com", result.getEmail());
    }

    @Test
    public void testUpdateUserInvalidUserDTOReturnsNull() {
        Long userId = 1L;
        when(userDAO.findById(userId)).thenReturn(Optional.empty());

        UserDTO userDTO = new UserDTO(userId, "Updated", "User", "updated@example.com");
        UserDTO result = userService.updateUser(userDTO);
        assertNull(result);
    }

    @Test
    public void testDeleteUserValidIdDeletesExistingUser() {
        Long userId = 1L;
        User existingUser = new User(userId, "John", "Doe", "john@example.com");
        when(userDAO.findById(userId)).thenReturn(Optional.of(existingUser));

        boolean result = userService.deleteUser(userId);
        assertTrue(result);
        verify(userDAO, times(1)).delete(existingUser);
    }

    @Test
    public void testDeleteUserInvalidIdReturnsFalse() {
        Long userId = 1L;
        when(userDAO.findById(userId)).thenReturn(Optional.empty());

        boolean result = userService.deleteUser(userId);
        assertFalse(result);
        verify(userDAO, never()).delete(any(User.class));
    }

    @Test
    public void testDeleteUserNullIdReturnsFalse() {
        boolean result = userService.deleteUser(null);
        assertFalse(result);
        verify(userDAO, never()).delete(any(User.class));
    }

}