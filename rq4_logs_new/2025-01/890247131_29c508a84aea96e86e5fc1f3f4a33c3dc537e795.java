package org.cyberrealm.tech.repository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.cyberrealm.tech.util.TestConstants.FIRST_USER_EMAIL;
import static org.cyberrealm.tech.util.TestConstants.FIRST_USER_ID;
import static org.cyberrealm.tech.util.TestConstants.INVALID_USER_EMAIL;
import static org.cyberrealm.tech.util.TestConstants.INVALID_USER_ID;

import java.util.Optional;
import org.cyberrealm.tech.model.User;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.jdbc.Sql;

@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Sql(scripts = {
        "classpath:database/roles/add-roles.sql",
        "classpath:database/users/add-users.sql",
        "classpath:database/users_roles/add-users_roles.sql"
}, executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD)
@Sql(scripts = {
        "classpath:database/users_roles/remove-users_roles.sql",
        "classpath:database/users/remove-users.sql",
        "classpath:database/roles/remove-roles.sql"
}, executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
class UserRepositoryTest {
    @Autowired
    private UserRepository userRepository;

    @Test
    @DisplayName("Verify findById() method works with valid id")
    void findById_validId_returnsUser() {
        // When
        Optional<User> foundUser = userRepository.findById(FIRST_USER_ID);
        // Then
        assertThat(foundUser).isPresent();
        assertThat(foundUser.get().getId()).isEqualTo(FIRST_USER_ID);
    }

    @Test
    @DisplayName("Verify findById() method works with invalid id")
    void findById_invalidId_returnsEmpty() {
        // When
        Optional<User> foundUser = userRepository.findById(INVALID_USER_ID);
        // Then
        assertThat(foundUser).isEmpty();
    }

    @Test
    @DisplayName("Verify findByEmail() method works with valid email")
    void findByEmail_validEmail_returnsUser() {
        // When
        Optional<User> foundUser = userRepository.findByEmail(FIRST_USER_EMAIL);
        // Then
        assertThat(foundUser).isPresent();
        assertThat(foundUser.get().getEmail()).isEqualTo(FIRST_USER_EMAIL);
    }

    @Test
    @DisplayName("Verify findByEmail() method works with invalid email")
    void findByEmail_invalidEmail_returnsEmpty() {
        // When
        Optional<User> foundUser = userRepository.findByEmail(INVALID_USER_EMAIL);
        // Then
        assertThat(foundUser).isEmpty();
    }
}