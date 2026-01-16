package org.cyberrealm.tech.service.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.cyberrealm.tech.util.TestConstants.SESSION_ID;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.stripe.exception.ApiException;
import com.stripe.model.checkout.Session;
import com.stripe.model.checkout.SessionCollection;
import com.stripe.param.checkout.SessionCreateParams;
import com.stripe.param.checkout.SessionListParams;
import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import org.cyberrealm.tech.dto.stripe.DescriptionForStripeDto;
import org.cyberrealm.tech.exception.StripeProcessingException;
import org.cyberrealm.tech.model.Booking;
import org.cyberrealm.tech.model.Payment;
import org.cyberrealm.tech.repository.PaymentRepository;
import org.cyberrealm.tech.repository.booking.BookingRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class StripeServiceTest {

    @Mock
    private PaymentRepository paymentRepository;

    @Mock
    private BookingRepository bookingRepository;

    @InjectMocks
    private StripeService stripeService;

    private Payment payment;
    private Booking booking;
    private DescriptionForStripeDto stripeDto;

    @BeforeEach
    void setUp() {
        booking = new Booking();
        booking.setId(1L);

        payment = new Payment();
        payment.setId(1L);
        payment.setStatus(Payment.PaymentStatus.EXPIRED);
        payment.setBooking(booking);
        payment.setAmountToPay(BigDecimal.valueOf(100));

        stripeDto = new DescriptionForStripeDto(1L, BigDecimal.valueOf(100), "Booking #1");
    }

    @Test
    void createStripeSession_validStripeDto_returnsSession() {
        Session session = new Session();
        try (MockedStatic<Session> mockedSession = Mockito.mockStatic(Session.class)) {
            mockedSession.when(() -> Session.create(any(SessionCreateParams.class)))
                    .thenReturn(session);

            Session actualSession = stripeService.createStripeSession(stripeDto);

            assertThat(actualSession).isNotNull();
            mockedSession.verify(() -> Session.create(any(SessionCreateParams.class)),
                    times(1));
        }
    }

    @Test
    void createStripeSession_stripeException_throwsStripeProcessingException() {
        try (MockedStatic<Session> mockedSession = Mockito.mockStatic(Session.class)) {
            mockedSession.when(() -> Session.create(any(SessionCreateParams.class)))
                    .thenThrow(new ApiException("Test exception", null, null, 0, null));

            assertThrows(StripeProcessingException.class, () ->
                    stripeService.createStripeSession(stripeDto));

            mockedSession.verify(() -> Session.create(any(SessionCreateParams.class)),
                    times(1));
        }
    }

    @Test
    void renewSession_expiredPayment_returnsSession() {
        Session session = new Session();
        try (MockedStatic<Session> mockedSession = Mockito.mockStatic(Session.class)) {
            mockedSession.when(() -> Session.create(any(SessionCreateParams.class)))
                    .thenReturn(session);

            Session actualSession = stripeService.renewSession(payment);

            assertThat(actualSession).isNotNull();
            mockedSession.verify(() -> Session.create(any(SessionCreateParams.class)),
                    times(1));
        }
    }

    @Test
    void renewSession_nonExpiredPayment_throwsStripeProcessingException() {
        payment.setStatus(Payment.PaymentStatus.PAID);

        assertThrows(StripeProcessingException.class, () -> stripeService.renewSession(payment));
    }

    @Test
    void checkExpiredSession_validParams_handlesExpiredSessions() {
        Session session = new Session();
        session.setId(SESSION_ID);
        session.setStatus("expired");
        SessionCollection sessionCollection = new SessionCollection();
        sessionCollection.setData(List.of(session));

        try (MockedStatic<Session> mockedSession = Mockito.mockStatic(Session.class)) {
            mockedSession.when(() -> Session.list(any(SessionListParams.class)))
                    .thenReturn(sessionCollection);
            when(paymentRepository.existsBySessionId(SESSION_ID)).thenReturn(true);
            when(paymentRepository.findBookingBySessionId(anyString()))
                    .thenReturn(Optional.of(booking));
            when(bookingRepository.save(any(Booking.class))).thenReturn(booking);

            stripeService.checkExpiredSession();

            verify(paymentRepository, times(1))
                    .existsBySessionId(anyString());
            verify(paymentRepository, times(1))
                    .findBookingBySessionId(anyString());
            verify(bookingRepository, times(1)).save(any(Booking.class));
        }
    }

    @Test
    void checkExpiredSession_stripeException_throwsStripeProcessingException() {
        try (MockedStatic<Session> mockedSession = Mockito.mockStatic(Session.class)) {
            mockedSession.when(() -> Session.list(any(SessionListParams.class)))
                    .thenThrow(new ApiException("Test exception", null, null, 0, null));

            assertThrows(StripeProcessingException.class, () ->
                    stripeService.checkExpiredSession());
        }
    }
}