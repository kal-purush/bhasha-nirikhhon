package com.dalevents.vensy.unit.repositories;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.util.Optional;

import org.assertj.core.internal.Objects;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase.Replace;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;

import com.dalevents.vensy.models.AppUser;
import com.dalevents.vensy.models.enums.Role;
import com.dalevents.vensy.repositories.AppUserRepository;

@DataJpaTest
@AutoConfigureTestDatabase(replace = Replace.NONE)
@TestInstance(Lifecycle.PER_CLASS)
public class AppUserRepositoryTests {
    @Autowired
    private AppUserRepository appUserRepository;

    AppUser testUser;
    String nonExistingEmail = "nonExists@gmaail.com";

    @BeforeAll
    void setUp() {
        testUser = AppUser.builder().email("test@gmail.com").firstname("FIRSTNAME").lastname("LASTNAME")
                .role(Role.COMPANY).build();
        testUser = appUserRepository.save(testUser);
    }

    @AfterAll
    void cleanUp() {
        appUserRepository.delete(testUser);
    }

    @Test
    void whenGivenAnExistingEmail_thenReturnTrue() {
        boolean result = appUserRepository.existsByEmail(testUser.getEmail());
        assertTrue(result);
    }

    @Test
    void whenGivenAnInvalidEmail_thenReturnFalse() {
        boolean result = appUserRepository.existsByEmail(nonExistingEmail);
        assertFalse(result);
    }

    @Test
    void whenGivenAnExistingEmail_thenReturnTheUser() {
        Optional<AppUser> result = appUserRepository.findByEmail(testUser.getEmail());
        assertTrue(result.isPresent());
        assertAll("Test testUser and result equality", () -> assertEquals(testUser.getId(), result.get().getId()),
                () -> assertEquals(testUser.getEmail(), result.get().getEmail()));
        ;
    }
}