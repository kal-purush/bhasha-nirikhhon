package org.cyberrealm.tech.repository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.cyberrealm.tech.util.TestConstants.FIRST_ROLE_NAME;
import static org.cyberrealm.tech.util.TestConstants.INVALID_ROLE_NAME;

import java.util.Optional;
import org.cyberrealm.tech.model.Role;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.jdbc.Sql;

@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Sql(scripts = {
        "classpath:database/roles/add-role-customer.sql"
}, executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD)
@Sql(scripts = {
        "classpath:database/roles/remove-roles.sql"
}, executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
class RoleRepositoryTest {
    @Autowired
    private RoleRepository roleRepository;

    @Test
    @DisplayName("Verify findByRole() method works with valid role")
    void findByRole_validRole_returnsRole() {
        // When
        Optional<Role> foundRole = roleRepository.findByRole(FIRST_ROLE_NAME);
        // Then
        assertThat(foundRole).isPresent();
        assertThat(foundRole.get().getRole()).isEqualTo(FIRST_ROLE_NAME);
    }

    @Test
    @DisplayName("Verify findByRole() method works with invalid role")
    void findByRole_invalidRole_returnsEmpty() {
        // When
        Optional<Role> foundRole = roleRepository.findByRole(INVALID_ROLE_NAME);
        // Then
        assertThat(foundRole).isEmpty();
    }
}