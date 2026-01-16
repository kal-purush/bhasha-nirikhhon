package org.wecancodeit.backend.DTOs;

/**
 * Data Transfer Object (DTO) for handling login credentials.
 * This class is used to encapsulate and transport user login data, including
 * the username and password, between client and server.
 */
public class LoginDTO {

    // User's password for authentication
    private String password;

    // User's username for authentication
    private String username;

    /**
     * Default no-argument constructor.
     * Required for frameworks and libraries that utilize reflection to instantiate objects.
     */
    public LoginDTO() {
        super();
    }

    /**
     * Parameterized constructor to create a LoginDTO with specific username and password.
     *
     * @param password The user's password for authentication.
     * @param username The user's username for authentication.
     */
    public LoginDTO(String password, String username) {
        this.password = password;
        this.username = username;
    }

    /**
     * Retrieves the password of the user.
     *
     * @return The user's password.
     */
    public String getPassword() {
        return password;
    }

    /**
     * Sets the user's password.
     *
     * @param password The password to set for the user.
     */
    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * Retrieves the username of the user.
     *
     * @return The user's username.
     */
    public String getUsername() {
        return username;
    }

    /**
     * Sets the username of the user.
     *
     * @param username The username to set for the user.
     */
    public void setUsername(String username) {
        this.username = username;
    }
}