package org.cyberrealm.tech.repository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.cyberrealm.tech.util.TestConstants.FIRST_BOOKING_ID;
import static org.cyberrealm.tech.util.TestConstants.FIRST_PAYMENT_ID;
import static org.cyberrealm.tech.util.TestConstants.FIRST_USER_ID;
import static org.cyberrealm.tech.util.TestConstants.INVALID_BOOKING_ID;
import static org.cyberrealm.tech.util.TestConstants.INVALID_PAYMENT_ID;
import static org.cyberrealm.tech.util.TestConstants.INVALID_SESSION_ID;
import static org.cyberrealm.tech.util.TestConstants.INVALID_USER_ID;
import static org.cyberrealm.tech.util.TestConstants.SESSION_ID;

import java.util.Optional;
import org.cyberrealm.tech.model.Booking;
import org.cyberrealm.tech.model.Payment;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.jdbc.Sql;

@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Sql(scripts = {
        "classpath:database/users/add-users.sql",
        "classpath:database/addresses/add-addresses.sql",
        "classpath:database/accommodations/add-accommodations.sql",
        "classpath:database/bookings/add-bookings.sql",
        "classpath:database/payments/add-payments.sql"
}, executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD)
@Sql(scripts = {
        "classpath:database/payments/remove-payments.sql",
        "classpath:database/bookings/remove-bookings.sql",
        "classpath:database/accommodations/remove-accommodations.sql",
        "classpath:database/addresses/remove-addresses.sql",
        "classpath:database/users/remove-users.sql"
}, executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
class PaymentRepositoryTest {
    @Autowired
    private PaymentRepository paymentRepository;

    @Test
    @DisplayName("Verify findBySessionId() method works with valid sessionId")
    void findBySessionId_validSessionId_returnsPayment() {
        // When
        Optional<Payment> foundPayment = paymentRepository.findBySessionId(SESSION_ID);
        // Then
        assertThat(foundPayment).isPresent();
        assertThat(foundPayment.get().getSessionId()).isEqualTo(SESSION_ID);
    }

    @Test
    @DisplayName("Verify findBySessionId() method works with invalid sessionId")
    void findBySessionId_invalidSessionId_returnsEmpty() {
        // When
        Optional<Payment> foundPayment = paymentRepository.findBySessionId(INVALID_SESSION_ID);
        // Then
        assertThat(foundPayment).isEmpty();
    }

    @Test
    @DisplayName("Verify findByBookingId() method works with valid bookingId")
    void findByBookingId_validBookingId_returnsPayment() {
        // When
        Optional<Payment> foundPayment = paymentRepository.findByBookingId(FIRST_BOOKING_ID);
        // Then
        assertThat(foundPayment).isPresent();
        assertThat(foundPayment.get().getBooking().getId()).isEqualTo(FIRST_BOOKING_ID);
    }

    @Test
    @DisplayName("Verify findByBookingId() method works with invalid bookingId")
    void findByBookingId_invalidBookingId_returnsEmpty() {
        // When
        Optional<Payment> foundPayment = paymentRepository.findByBookingId(INVALID_BOOKING_ID);
        // Then
        assertThat(foundPayment).isEmpty();
    }

    @Test
    @DisplayName("Verify existsBySessionId() method works with valid sessionId")
    void existsBySessionId_validSessionId_returnsTrue() {
        // When
        boolean exists = paymentRepository.existsBySessionId(SESSION_ID);
        // Then
        assertThat(exists).isTrue();
    }

    @Test
    @DisplayName("Verify existsBySessionId() method works with invalid sessionId")
    void existsBySessionId_invalidSessionId_returnsFalse() {
        // When
        boolean exists = paymentRepository.existsBySessionId(INVALID_SESSION_ID);
        // Then
        assertThat(exists).isFalse();
    }

    @Test
    @DisplayName("Verify existsPendingPaymentsByUserId() method works with valid userId")
    void existsPendingPaymentsByUserId_validUserId_returnsTrue() {
        // When
        boolean exists = paymentRepository.existsPendingPaymentsByUserId(FIRST_USER_ID);
        // Then
        assertThat(exists).isTrue();
    }

    @Test
    @DisplayName("Verify existsPendingPaymentsByUserId() method works with invalid userId")
    void existsPendingPaymentsByUserId_invalidUserId_returnsFalse() {
        // When
        boolean exists = paymentRepository.existsPendingPaymentsByUserId(INVALID_USER_ID);
        // Then
        assertThat(exists).isFalse();
    }

    @Test
    @DisplayName("Verify findBookingBySessionId() method works with valid sessionId")
    void findBookingBySessionId_validSessionId_returnsBooking() {
        // When
        Optional<Booking> foundBooking = paymentRepository.findBookingBySessionId(SESSION_ID);
        // Then
        assertThat(foundBooking).isPresent();
        assertThat(foundBooking.get().getId()).isEqualTo(FIRST_BOOKING_ID);
    }

    @Test
    @DisplayName("Verify findBookingBySessionId() method works with invalid sessionId")
    void findBookingBySessionId_invalidSessionId_returnsEmpty() {
        // When
        Optional<Booking> foundBooking = paymentRepository.findBookingBySessionId(
                INVALID_SESSION_ID);
        // Then
        assertThat(foundBooking).isEmpty();
    }

    @Test
    @DisplayName("Verify findByIdAndUserId() method works with valid paymentId and userId")
    void findByIdAndUserId_validPaymentIdAndUserId_returnsPayment() {
        // When
        Optional<Payment> foundPayment = paymentRepository.findByIdAndUserId(FIRST_PAYMENT_ID,
                FIRST_USER_ID);
        // Then
        assertThat(foundPayment).isPresent();
        assertThat(foundPayment.get().getId()).isEqualTo(FIRST_PAYMENT_ID);
    }

    @Test
    @DisplayName("Verify findByIdAndUserId() method works with invalid paymentId and userId")
    void findByIdAndUserId_invalidPaymentIdAndUserId_returnsEmpty() {
        // When
        Optional<Payment> foundPayment = paymentRepository.findByIdAndUserId(INVALID_PAYMENT_ID,
                INVALID_USER_ID);
        // Then
        assertThat(foundPayment).isEmpty();
    }
}