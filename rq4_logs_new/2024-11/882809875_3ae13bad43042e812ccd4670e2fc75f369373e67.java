package org.wecancodeit.backend;

import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.*;
import org.wecancodeit.backend.BOService.UserService;
import org.wecancodeit.backend.DataModels.UserModel;
import org.wecancodeit.backend.Repositories.UserRepository;

import java.util.List;
import java.util.Optional;

public class UserServiceTest {

    @Mock
    private UserRepository userRepository;  // Mock the UserRepository dependency

    private UserService userService; // Instance of the service to test

    // This method runs before each test method to set up the mock and the service instance.
    @BeforeEach
    public void setUp() {
        // Initialize the mock objects and the UserService instance before each test
        MockitoAnnotations.openMocks(this);
        userService = new UserService(userRepository);  // Create an instance of UserService with the mocked UserRepository
    }

    // Test case for the `getAll()` method, which retrieves all users.
    @Test
    public void testGetAll() {
        // Given: A list of users is mocked
        UserModel user1 = new UserModel(1L, "user1", "password123", 25);
        UserModel user2 = new UserModel(2L, "user2", "password456", 30);
        List<UserModel> users = List.of(user1, user2);

        // When: The `findAll()` method of the repository is mocked to return the users list
        when(userRepository.findAll()).thenReturn(users);

        // Then: Verify that the result matches the mocked users
        List<UserModel> result = userService.getAll();
        assertEquals(2, result.size());  // Ensure we have 2 users in the list
        assertEquals("user1", result.get(0).getUsername());  // First user should be "user1"
        assertEquals("user2", result.get(1).getUsername());  // Second user should be "user2"
    }

    // Test case for registering a new user, ensuring it works correctly.
    @Test
    public void testRegisterUser_Success() throws Exception {
        // Given: A new user to register
        UserModel newUser = new UserModel(3L, "newUser", "newPass123", 22);
        
        // When: The repository is mocked to return no user with the same username (indicating the username is available)
        when(userRepository.findByUsernameIgnoreCase("newUser")).thenReturn(List.of());  // No existing user with this username
        when(userRepository.save(newUser)).thenReturn(newUser);  // Mock the save operation to return the new user

        // Then: Ensure the new user is registered correctly
        UserModel registeredUser = userService.registerUser(newUser);
        assertNotNull(registeredUser);  // Ensure that the registered user is not null
        assertEquals("newUser", registeredUser.getUsername());  // The username should match the new user's username
    }

    // Test case for registering a user with a duplicate username.
    @Test
    public void testRegisterUser_Failure_DuplicateUsername() {
        // Given: A new user that attempts to register with a duplicate username
        UserModel newUser = new UserModel(3L, "existingUser", "password123", 25);
        List<UserModel> existingUserList = List.of(new UserModel(1L, "existingUser", "oldPass123", 28));  // Mock an existing user with the same username

        // When: The repository is mocked to return the existing user when searching by username
        when(userRepository.findByUsernameIgnoreCase("existingUser")).thenReturn(existingUserList);

        // Then: The `registerUser` method should throw an exception due to the duplicate username
        Exception exception = assertThrows(Exception.class, () -> userService.registerUser(newUser));
        assertEquals("Username is taken. Please use another. ", exception.getMessage());  // Verify the exception message
    }

    // Test case for finding a user by their username.
    @Test
    public void testFindUserByUsername() {
        // Given: A user with a specific username
        UserModel user = new UserModel(1L, "user1", "password123", 25);
        List<UserModel> users = List.of(user);  // Create a list with the mock user

        // When: The `findByUsernameIgnoreCase()` method is mocked to return the user list
        when(userRepository.findByUsernameIgnoreCase("user1")).thenReturn(users);

        // Then: Verify that the result matches the mocked user
        List<UserModel> result = userService.findUserByUsername("user1");
        assertEquals(1, result.size());  // Ensure that exactly one user is returned
        assertEquals("user1", result.get(0).getUsername());  // Verify the username of the returned user
    }

    // Test case for finding a user by ID, ensuring the user is found.
    @Test
    public void testFindUserById_Found() {
        // Given: A user with a specific ID
        UserModel user = new UserModel(1L, "user1", "password123", 25);
        Optional<UserModel> optionalUser = Optional.of(user);  // Wrap the user in an Optional

        // When: The `findById()` method is mocked to return the user
        when(userRepository.findById(1L)).thenReturn(optionalUser);

        // Then: Verify that the user is found and the username matches
        Optional<UserModel> result = userService.findUserById(1L);
        assertTrue(result.isPresent());  // Ensure the user is found (Optional is not empty)
        assertEquals("user1", result.get().getUsername());  // Verify the username of the found user
    }

    // Test case for finding a user by ID, ensuring the user is not found.
    @Test
    public void testFindUserById_NotFound() {
        // Given: No user exists with the specified ID
        when(userRepository.findById(999L)).thenReturn(Optional.empty());  // Mock an empty Optional

        // When: The `findUserById()` method is called with a non-existing ID
        Optional<UserModel> result = userService.findUserById(999L);

        // Then: Verify that the result is empty (user not found)
        assertFalse(result.isPresent());  // The result should be empty if no user is found
    }

    // Test case for updating a user's information.
    @Test
    public void testUpdateUser() {
        // Given: An existing user whose information needs to be updated
        UserModel user = new UserModel(1L, "user1", "password123", 25);
        when(userRepository.save(user)).thenReturn(user);  // Mock the save operation to return the updated user

        // When: The `updateUser()` method is called
        UserModel updatedUser = userService.updateUser(user);

        // Then: Verify that the returned user is not null and has the correct username
        assertNotNull(updatedUser);  // Ensure the updated user is not null
        assertEquals("user1", updatedUser.getUsername());  // The username should match the updated user
    }

    // Test case for deleting a user by their ID.
    @Test
    public void testDeleteUser() {
        // Given: A user ID that needs to be deleted
        long userId = 1L;

        // When: The `deleteById()` method is mocked to do nothing (i.e., successfully deleting the user)
        doNothing().when(userRepository).deleteById(userId);

        // Then: Verify that the `deleteById()` method was called exactly once
        userService.deleteUser(userId);
        verify(userRepository, times(1)).deleteById(userId);  // Ensure the deleteById method was called once with the correct ID
    }
}