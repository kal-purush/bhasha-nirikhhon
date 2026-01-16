package org.cyberrealm.tech.repository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.cyberrealm.tech.util.TestConstants.FIRST_ACCOMMODATION_ID;
import static org.cyberrealm.tech.util.TestConstants.INVALID_ACCOMMODATION_ID;
import static org.cyberrealm.tech.util.TestUtil.FIRST_ACCOMMODATION;
import static org.cyberrealm.tech.util.TestUtil.PAGEABLE;

import java.util.Optional;
import org.cyberrealm.tech.model.Accommodation;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.data.domain.Page;
import org.springframework.test.context.jdbc.Sql;

@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Sql(scripts = {
        "classpath:database/addresses/add-addresses.sql",
        "classpath:database/accommodations/add-accommodations.sql"
}, executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD)
@Sql(scripts = {
        "classpath:database/accommodations/remove-accommodations.sql",
        "classpath:database/addresses/remove-addresses.sql"
}, executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
class AccommodationRepositoryTest {
    @Autowired
    private AccommodationRepository accommodationRepository;

    @Test
    @DisplayName("Verify findById() method works with valid data")
    void findById_validId_returnsAccommodation() {
        // When
        Optional<Accommodation> foundAccommodation = accommodationRepository.findById(
                FIRST_ACCOMMODATION_ID
        );
        // Then
        assertThat(foundAccommodation).isPresent();
        assertThat(foundAccommodation.get().getId()).isEqualTo(FIRST_ACCOMMODATION_ID);
    }

    @Test
    @DisplayName("Verify findById() method works with invalid data")
    void findById_invalidId_returnsEmpty() {
        // When
        Optional<Accommodation> foundAccommodation = accommodationRepository.findById(
                INVALID_ACCOMMODATION_ID
        );
        // Then
        assertThat(foundAccommodation).isEmpty();
    }

    @Test
    @DisplayName("Verify findAll(Pageable) method works")
    void findAll_pageable_returnsAccommodationsPage() {
        // When
        Page<Accommodation> accommodationPage = accommodationRepository.findAll(PAGEABLE);
        // Then
        assertThat(accommodationPage.getContent()).isNotEmpty();
        assertThat(accommodationPage.getContent().get(0).getId())
                .isEqualTo(FIRST_ACCOMMODATION.getId());
    }
}