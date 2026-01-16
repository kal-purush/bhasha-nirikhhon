package org.cyberrealm.tech.repository.booking;

import static org.assertj.core.api.Assertions.assertThat;
import static org.cyberrealm.tech.util.TestConstants.FIRST_ACCOMMODATION_ID;
import static org.cyberrealm.tech.util.TestConstants.FIRST_BOOKING_ID;
import static org.cyberrealm.tech.util.TestConstants.FIRST_USER_ID;
import static org.cyberrealm.tech.util.TestConstants.INVALID_BOOKING_ID;
import static org.cyberrealm.tech.util.TestConstants.NUMBER_OF_DAYS;
import static org.cyberrealm.tech.util.TestUtil.BOOKING_SPECIFICATION;
import static org.cyberrealm.tech.util.TestUtil.PAGEABLE;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import org.cyberrealm.tech.model.Booking;
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
        "classpath:database/users/add-users.sql",
        "classpath:database/accommodations/add-accommodations.sql",
        "classpath:database/bookings/add-bookings.sql"
}, executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD)
@Sql(scripts = {
        "classpath:database/bookings/remove-bookings.sql",
        "classpath:database/users/remove-users.sql",
        "classpath:database/accommodations/remove-accommodations.sql",
        "classpath:database/addresses/remove-addresses.sql",
}, executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
class BookingRepositoryTest {
    @Autowired
    private BookingRepository bookingRepository;

    @Test
    @DisplayName("Verify findById() method works with valid data")
    void findById_validId_returnsBooking() {
        // When
        Optional<Booking> foundBooking = bookingRepository.findById(FIRST_BOOKING_ID);
        // Then
        assertThat(foundBooking).isPresent();
        assertThat(foundBooking.get().getId()).isEqualTo(FIRST_BOOKING_ID);
    }

    @Test
    @DisplayName("Verify findById() method works with invalid data")
    void findById_invalidId_returnsEmpty() {
        // When
        Optional<Booking> foundBooking = bookingRepository.findById(INVALID_BOOKING_ID);
        // Then
        assertThat(foundBooking).isEmpty();
    }

    @Test
    @DisplayName("Verify findAllByUserId() method works")
    void findAllByUserId_validUserId_returnsBookings() {
        // When
        List<Booking> bookings = bookingRepository.findAllByUserId(FIRST_USER_ID);
        // Then
        assertThat(bookings).isNotEmpty();
        assertThat(bookings.get(0).getUser().getId()).isEqualTo(FIRST_USER_ID);
    }

    @Test
    @DisplayName("Verify findByUserId() method works with pageable")
    void findByUserId_pageable_returnsBookingsPage() {
        // When
        List<Booking> bookingsPage = bookingRepository.findByUserId(FIRST_USER_ID, PAGEABLE);
        // Then
        assertThat(bookingsPage).isNotEmpty();
        assertThat(bookingsPage.get(0).getUser().getId()).isEqualTo(FIRST_USER_ID);
    }

    @Test
    @DisplayName("Verify findAll(Specification, Pageable) method works")
    void findAll_withSpecificationAndPageable_returnsBookingsPage() {
        // When
        Page<Booking> bookingPage = bookingRepository.findAll(BOOKING_SPECIFICATION, PAGEABLE);
        // Then
        assertThat(bookingPage.getContent()).isNotEmpty();
        assertThat(bookingPage.getContent().get(0).getUser().getId()).isEqualTo(FIRST_USER_ID);
    }

    @Test
    @DisplayName("Verify countOverlappingBookings() method works with overlapping dates")
    void countOverlappingBookings_withOverlappingDates_returnsCount() {
        // When
        int count = bookingRepository.countOverlappingBookings(FIRST_ACCOMMODATION_ID,
                LocalDate.now().minusDays(1), LocalDate.now().plusDays(1));
        // Then
        assertThat(count).isGreaterThan(0);
    }

    @Test
    @DisplayName("Verify findAllByCheckOutDateBeforeAndStatusNot() method works")
    void findAllByCheckOutDateBeforeAndStatusNot_validData_returnsBookings() {
        // When
        List<Booking> bookings = bookingRepository.findAllByCheckOutDateBeforeAndStatusNot(
                LocalDate.now().plusDays(1), Booking.BookingStatus.CANCELED
        );
        // Then
        assertThat(bookings).isNotEmpty();
        assertThat(bookings.get(0).getStatus()).isNotEqualTo(Booking.BookingStatus.CANCELED);
    }

    @Test
    @DisplayName("Verify numberOfDays() method works")
    void numberOfDays_validBookingId_returnsDays() {
        // When
        int days = bookingRepository.numberOfDays(FIRST_BOOKING_ID);
        // Then
        assertThat(days).isEqualTo(NUMBER_OF_DAYS);
    }
}