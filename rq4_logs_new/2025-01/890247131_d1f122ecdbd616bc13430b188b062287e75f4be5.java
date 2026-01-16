package org.cyberrealm.tech.util;

import java.math.BigDecimal;
import java.time.LocalDate;
import org.cyberrealm.tech.dto.stripe.DescriptionForStripeDto;
import org.cyberrealm.tech.model.Payment;
import org.cyberrealm.tech.model.Role;

public class TestConstants {
    //User
    public static final String FIRST_USER_EMAIL = "test@example.com";
    public static final String SECOND_USER_EMAIL = "maks@example.com";
    public static final String INVALID_USER_EMAIL = "invalid@example.com";
    public static final String FIRST_USER_FIRST_NAME = "Mike";
    public static final String FIRST_USER_LAST_NAME = "Johnston";
    public static final String NEW_ROLE = "ROLE_MANAGER";
    public static final String NEW_FIRST_NAME = "John";
    public static final String NEW_LAST_NAME = "Diesel";
    //Accommodation
    public static final Long FIRST_ACCOMMODATION_ID = 1L;
    public static final Long SECOND_ACCOMMODATION_ID = 2L;
    public static final Long INVALID_ACCOMMODATION_ID = 99999L;
    public static final String ACCOMMODATION_TYPE_HOUSE = "HOUSE";
    public static final int FIRST_AVAILABILITY = 1;
    public static final BigDecimal DAILY_RATE_23 = BigDecimal.valueOf(23.00);
    public static final String STUDIO = "Studio";
    public static final String ENTITY_NOT_FOUND_EXCEPTION = "Can't find %s by id: %d";

    //Address
    public static final Long FIRST_ADDRESS_ID = 1L;
    public static final Long SECOND_ADDRESS_ID = 2L;
    public static final String ADDRESS_DUPLICATE_RESOURCE_EXCEPTION = "This address %s,%s,%s,%s,%s"
            + ",%s already exists";
    public static final String BOOKED_ADDRESS_DUPLICATE_RESOURCE_EXCEPTION = "This address %s,%s,"
            + "%s,%s,%s,%s already belong another accommodation";

    public static final long FIRST_BOOKING_ID = 1L;
    public static final long SECOND_BOOKING_ID = 2L;
    public static final long INVALID_BOOKING_ID = 99999L;
    public static final String BOOKING_ENTITY_NOT_FOUND_EXCEPTION = "Can't find booking by id: %d";
    public static final String ACCOMMODATION = "accommodation";
    public static final long FIRST_USER_ID = 1L;
    public static final String FIRST_USER_PASSWORD = "password";
    public static final String ENCODED_PASSWORD = "encodedPassword";
    public static final String USER_WITH_PENDING_PAYMENTS_EXCEPTION = "User with id:%d has pending "
            + "payments and cannot create new booking.";
    public static final String NEW_BOOKING_NOTIFICATION = "New booking created with ID:%d";
    public static final String INVALID_ACCOMMODATION_EXCEPTION = "Can't find accommodation by id:"
            + "%d";

    // Payment
    public static final long FIRST_PAYMENT_ID = 1L;
    public static final long SECOND_PAYMENT_ID = 2L;
    public static final long EXPIRED_PAYMENT_ID = 3L;
    public static final String SESSION_ID = "sessionId";
    public static final String INVALID_SESSION_ID = "123anotherSessionId123";
    public static final String EXPIRED_SESSION_ID = "anotherSessionId123";
    public static final String SECOND_SESSION_ID = "anotherSessionId";
    public static final String SESSION_URL = "http://example.com/session";
    public static final BigDecimal PAYMENT_AMOUNT = BigDecimal.valueOf(230.00);
    public static final BigDecimal AMOUNT_TO_PAY = BigDecimal.valueOf(1000.00);
    public static final Payment.PaymentStatus PAYMENT_STATUS = Payment.PaymentStatus.PENDING;
    public static final String EXPIRED = "EXPIRED";
    public static final String PENDING = "PENDING";
    public static final int NUMBER_OF_DAYS = 10;

    public static final DescriptionForStripeDto NEW_STRIPE_DTO = new DescriptionForStripeDto(
            FIRST_BOOKING_ID,
            PAYMENT_AMOUNT,
            "New Stripe Session"
    );
    public static final DescriptionForStripeDto RENEWAL_STRIPE_DTO = new DescriptionForStripeDto(
            FIRST_BOOKING_ID,
            PAYMENT_AMOUNT,
            "Renewal for booking #" + FIRST_BOOKING_ID
    );

    public static final Long INVALID_ADDRESS_ID = 99999L;

    // Payment
    public static final Long INVALID_PAYMENT_ID = 99999L;
    public static final boolean IS_DELETED = false;

    // User
    public static final Long INVALID_USER_ID = 99999L;

    public static final Role.RoleName FIRST_ROLE_NAME = Role.RoleName.ROLE_CUSTOMER;
    public static final Role.RoleName INVALID_ROLE_NAME = Role.RoleName.ROLE_MANAGER;
    public static final LocalDate CHECK_IN_DATE = LocalDate.now();
    public static final LocalDate CHECK_OUT_DATE = LocalDate.now().plusDays(10);
    public static final LocalDate NEW_CHECK_IN_DATE = LocalDate.now().plusDays(35);
    public static final LocalDate NEW_CHECK_OUT_DATE = LocalDate.now().plusDays(40);
    public static final String POOL = "pool";
    public static final String ELECTRICITY = "electricity";
    public static final String WIFI = "wifi";

    // Payment
    public static final BigDecimal AMOUNT = new BigDecimal("100.00");
    public static final String CURRENCY = "USD";

    public static final String USER_EMAIL = "sych@example.com";
    public static final String USER_PASSWORD = "password";
    public static final String USER_FIRST_NAME = "Oleksandr";
    public static final String USER_LAST_NAME = "Sych";
    public static final String USER_ROLE = "ROLE_CUSTOMER";
}