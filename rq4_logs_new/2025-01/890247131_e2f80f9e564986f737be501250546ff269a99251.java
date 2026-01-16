package org.cyberrealm.tech.repository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.cyberrealm.tech.util.TestUtil.FIRST_ADDRESS;
import static org.cyberrealm.tech.util.TestUtil.INVALID_ADDRESS;

import org.cyberrealm.tech.model.Address;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.jdbc.Sql;

@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Sql(scripts = {
        "classpath:database/addresses/add-addresses.sql"
}, executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD)
@Sql(scripts = {
        "classpath:database/addresses/remove-addresses.sql"
}, executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
class AddressRepositoryTest {
    @Autowired
    private AddressRepository addressRepository;

    @Test
    @DisplayName("Verify existsByCountryAndCityAndStateAndStreetAndHouseNumber() method works "
            + "with valid data")
    void existsByCountryAndCityAndStateAndStreetAndHouseNumber_validData_returnsTrue() {
        // When
        boolean exists = addressRepository.existsByCountryAndCityAndStateAndStreetAndHouseNumber(
                FIRST_ADDRESS.getCountry(),
                FIRST_ADDRESS.getCity(),
                FIRST_ADDRESS.getState(),
                FIRST_ADDRESS.getStreet(),
                FIRST_ADDRESS.getHouseNumber()
        );
        // Then
        assertThat(exists).isTrue();
    }

    @Test
    @DisplayName("Verify existsByCountryAndCityAndStateAndStreetAndHouseNumber() method works "
            + "with invalid data")
    void existsByCountryAndCityAndStateAndStreetAndHouseNumber_invalidData_returnsFalse() {
        // When
        boolean exists = addressRepository.existsByCountryAndCityAndStateAndStreetAndHouseNumber(
                INVALID_ADDRESS.getCountry(),
                INVALID_ADDRESS.getCity(),
                INVALID_ADDRESS.getState(),
                INVALID_ADDRESS.getStreet(),
                INVALID_ADDRESS.getHouseNumber()
        );
        // Then
        assertThat(exists).isFalse();
    }

    @Test
    @DisplayName("Verify findByCountryAndCityAndStateAndStreetAndHouseNumber() method works "
            + "with valid data")
    void findByCountryAndCityAndStateAndStreetAndHouseNumber_validData_returnsAddress() {
        // When
        Address foundAddress =
                addressRepository.findByCountryAndCityAndStateAndStreetAndHouseNumber(
                        FIRST_ADDRESS.getCountry(),
                        FIRST_ADDRESS.getCity(),
                        FIRST_ADDRESS.getState(),
                        FIRST_ADDRESS.getStreet(),
                        FIRST_ADDRESS.getHouseNumber()
                );
        // Then
        assertThat(foundAddress).isNotNull();
        assertThat(foundAddress.getId()).isEqualTo(FIRST_ADDRESS.getId());
    }

    @Test
    @DisplayName("Verify findByCountryAndCityAndStateAndStreetAndHouseNumber() method works "
            + "with invalid data")
    void findByCountryAndCityAndStateAndStreetAndHouseNumber_invalidData_returnsNull() {
        // When
        Address foundAddress =
                addressRepository.findByCountryAndCityAndStateAndStreetAndHouseNumber(
                        INVALID_ADDRESS.getCountry(),
                        INVALID_ADDRESS.getCity(),
                        INVALID_ADDRESS.getState(),
                        INVALID_ADDRESS.getStreet(),
                        INVALID_ADDRESS.getHouseNumber()
                );
        // Then
        assertThat(foundAddress).isNull();
    }
}