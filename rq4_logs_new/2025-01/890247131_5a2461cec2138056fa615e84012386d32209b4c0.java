package org.cyberrealm.tech.util;

import static org.cyberrealm.tech.util.TestConstants.ACCOMMODATION_TYPE_HOUSE;
import static org.cyberrealm.tech.util.TestConstants.CHECK_IN_DATE;
import static org.cyberrealm.tech.util.TestConstants.CHECK_OUT_DATE;
import static org.cyberrealm.tech.util.TestConstants.DAILY_RATE_23;
import static org.cyberrealm.tech.util.TestConstants.ELECTRICITY;
import static org.cyberrealm.tech.util.TestConstants.EXPIRED_PAYMENT_ID;
import static org.cyberrealm.tech.util.TestConstants.EXPIRED_SESSION_ID;
import static org.cyberrealm.tech.util.TestConstants.FIRST_ACCOMMODATION_ID;
import static org.cyberrealm.tech.util.TestConstants.FIRST_ADDRESS_ID;
import static org.cyberrealm.tech.util.TestConstants.FIRST_AVAILABILITY;
import static org.cyberrealm.tech.util.TestConstants.FIRST_BOOKING_ID;
import static org.cyberrealm.tech.util.TestConstants.FIRST_PAYMENT_ID;
import static org.cyberrealm.tech.util.TestConstants.FIRST_USER_ID;
import static org.cyberrealm.tech.util.TestConstants.INVALID_ACCOMMODATION_ID;
import static org.cyberrealm.tech.util.TestConstants.INVALID_ADDRESS_ID;
import static org.cyberrealm.tech.util.TestConstants.PAYMENT_AMOUNT;
import static org.cyberrealm.tech.util.TestConstants.PAYMENT_STATUS;
import static org.cyberrealm.tech.util.TestConstants.PENDING;
import static org.cyberrealm.tech.util.TestConstants.POOL;
import static org.cyberrealm.tech.util.TestConstants.SECOND_ACCOMMODATION_ID;
import static org.cyberrealm.tech.util.TestConstants.SECOND_ADDRESS_ID;
import static org.cyberrealm.tech.util.TestConstants.SECOND_BOOKING_ID;
import static org.cyberrealm.tech.util.TestConstants.SECOND_PAYMENT_ID;
import static org.cyberrealm.tech.util.TestConstants.SECOND_SESSION_ID;
import static org.cyberrealm.tech.util.TestConstants.SESSION_ID;
import static org.cyberrealm.tech.util.TestConstants.SESSION_URL;
import static org.cyberrealm.tech.util.TestConstants.STUDIO;
import static org.cyberrealm.tech.util.TestConstants.USER_EMAIL;
import static org.cyberrealm.tech.util.TestConstants.USER_FIRST_NAME;
import static org.cyberrealm.tech.util.TestConstants.USER_LAST_NAME;
import static org.cyberrealm.tech.util.TestConstants.USER_PASSWORD;
import static org.cyberrealm.tech.util.TestConstants.USER_ROLE;
import static org.cyberrealm.tech.util.TestConstants.WIFI;

import com.stripe.model.checkout.Session;
import com.stripe.param.checkout.SessionListParams;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.cyberrealm.tech.dto.accommodation.AccommodationDto;
import org.cyberrealm.tech.dto.accommodation.CreateAccommodationRequestDto;
import org.cyberrealm.tech.dto.address.CreateAddressRequestDto;
import org.cyberrealm.tech.dto.booking.BookingDto;
import org.cyberrealm.tech.dto.booking.BookingSearchParameters;
import org.cyberrealm.tech.dto.booking.CreateBookingRequestDto;
import org.cyberrealm.tech.dto.payment.CreatePaymentRequestDto;
import org.cyberrealm.tech.dto.payment.PaymentDto;
import org.cyberrealm.tech.dto.payment.PaymentWithoutSessionDto;
import org.cyberrealm.tech.dto.user.UserInfoUpdateDto;
import org.cyberrealm.tech.dto.user.UserLoginRequestDto;
import org.cyberrealm.tech.dto.user.UserRegistrationRequestDto;
import org.cyberrealm.tech.dto.user.UserRoleUpdateDto;
import org.cyberrealm.tech.model.Accommodation;
import org.cyberrealm.tech.model.Address;
import org.cyberrealm.tech.model.Booking;
import org.cyberrealm.tech.model.Payment;
import org.cyberrealm.tech.model.Role;
import org.cyberrealm.tech.model.User;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.jpa.domain.Specification;

public class TestUtil {
    //Amenities
    public static final List<String> AMENITIES = new ArrayList<>();

    static {
        AMENITIES.add(POOL);
        AMENITIES.add(ELECTRICITY);
        AMENITIES.add(WIFI);
    }

    //Address
    public static final Address FIRST_ADDRESS = new Address();

    static {
        FIRST_ADDRESS.setId(FIRST_ADDRESS_ID);
        FIRST_ADDRESS.setCountry("USA");
        FIRST_ADDRESS.setCity("Chicago");
        FIRST_ADDRESS.setState("Illinois");
        FIRST_ADDRESS.setStreet("Cicero");
        FIRST_ADDRESS.setHouseNumber("49th Ave");
        FIRST_ADDRESS.setPostalCode("60804");
    }

    public static final Address SECOND_ADDRESS = new Address();

    static {
        SECOND_ADDRESS.setId(SECOND_ADDRESS_ID);
        SECOND_ADDRESS.setCountry("Ukraine");
        SECOND_ADDRESS.setCity("Lviv");
        SECOND_ADDRESS.setState("Lvivskyi");
        SECOND_ADDRESS.setStreet("Shevchenka");
        SECOND_ADDRESS.setHouseNumber("25");
        SECOND_ADDRESS.setPostalCode("80352");
    }

    public static final CreateAddressRequestDto CREATE_ADDRESS_REQUEST_DTO =
            new CreateAddressRequestDto(
                    FIRST_ADDRESS.getCountry(),
                    FIRST_ADDRESS.getCity(),
                    FIRST_ADDRESS.getState(),
                    FIRST_ADDRESS.getStreet(),
                    FIRST_ADDRESS.getHouseNumber(),
                    FIRST_ADDRESS.getPostalCode()
            );

    public static final CreateAddressRequestDto SECOND_CREATE_ADDRESS_REQUEST_DTO =
            new CreateAddressRequestDto(
                    SECOND_ADDRESS.getCountry(),
                    SECOND_ADDRESS.getCity(),
                    SECOND_ADDRESS.getState(),
                    SECOND_ADDRESS.getStreet(),
                    SECOND_ADDRESS.getHouseNumber(),
                    SECOND_ADDRESS.getPostalCode()
            );

    //Accommodation
    public static final CreateAccommodationRequestDto CREATE_ACCOMMODATION_REQUEST_DTO =
            new CreateAccommodationRequestDto(
                    ACCOMMODATION_TYPE_HOUSE,
                    CREATE_ADDRESS_REQUEST_DTO,
                    STUDIO,
                    AMENITIES,
                    DAILY_RATE_23,
                    FIRST_AVAILABILITY
            );

    public static final CreateAccommodationRequestDto SECOND_CREATE_ACCOMMODATION_REQUEST_DTO =
            new CreateAccommodationRequestDto(
                    ACCOMMODATION_TYPE_HOUSE,
                    SECOND_CREATE_ADDRESS_REQUEST_DTO,
                    STUDIO,
                    AMENITIES,
                    DAILY_RATE_23,
                    FIRST_AVAILABILITY
            );
    public static final Accommodation FIRST_ACCOMMODATION_FROM_DTO = new Accommodation();

    static {
        FIRST_ACCOMMODATION_FROM_DTO.setType(Accommodation.Type.HOUSE);
        FIRST_ACCOMMODATION_FROM_DTO.setAddress(FIRST_ADDRESS);
        FIRST_ACCOMMODATION_FROM_DTO.setSize(STUDIO);
        FIRST_ACCOMMODATION_FROM_DTO.setAmenities(AMENITIES);
        FIRST_ACCOMMODATION_FROM_DTO.setDailyRate(DAILY_RATE_23);
        FIRST_ACCOMMODATION_FROM_DTO.setAvailability(FIRST_AVAILABILITY);
    }

    public static final Accommodation FIRST_ACCOMMODATION = new Accommodation();

    static {
        FIRST_ACCOMMODATION.setId(FIRST_ACCOMMODATION_ID);
        FIRST_ACCOMMODATION.setType(Accommodation.Type.HOUSE);
        FIRST_ACCOMMODATION.setAddress(FIRST_ADDRESS);
        FIRST_ACCOMMODATION.setSize(STUDIO);
        FIRST_ACCOMMODATION.setAmenities(AMENITIES);
        FIRST_ACCOMMODATION.setDailyRate(DAILY_RATE_23);
        FIRST_ACCOMMODATION.setAvailability(FIRST_AVAILABILITY);
    }

    public static final Accommodation SECOND_ACCOMMODATION = new Accommodation();

    static {
        SECOND_ACCOMMODATION.setId(SECOND_ACCOMMODATION_ID);
        SECOND_ACCOMMODATION.setType(Accommodation.Type.APARTMENT);
        SECOND_ACCOMMODATION.setAddress(SECOND_ADDRESS);
        SECOND_ACCOMMODATION.setSize(STUDIO);
        SECOND_ACCOMMODATION.setAmenities(AMENITIES);
        SECOND_ACCOMMODATION.setDailyRate(DAILY_RATE_23);
        SECOND_ACCOMMODATION.setAvailability(FIRST_AVAILABILITY);
    }

    public static final AccommodationDto ACCOMMODATION_RESPONSE_DTO =
            new AccommodationDto(
                    FIRST_ACCOMMODATION_ID,
                    ACCOMMODATION_TYPE_HOUSE,
                    FIRST_ADDRESS_ID,
                    STUDIO,
                    AMENITIES,
                    DAILY_RATE_23,
                    FIRST_AVAILABILITY
            );

    public static final Role CUSTOMER_ROLE = new Role();

    static {
        CUSTOMER_ROLE.setRole(Role.RoleName.ROLE_CUSTOMER);
    }

    public static final Role MANAGER_ROLE = new Role();

    static {
        MANAGER_ROLE.setRole(Role.RoleName.ROLE_MANAGER);
    }

    public static final User FIRST_USER = new User();

    static {
        FIRST_USER.setId(FIRST_USER_ID);
        FIRST_USER.setFirstName("Oleksandr");
        FIRST_USER.setLastName("Pavlyk");
        FIRST_USER.setEmail("oleksandr@ukr.net");
        FIRST_USER.setPassword("1234");
        FIRST_USER.setRoles(Set.of(MANAGER_ROLE));
    }

    public static final CreateBookingRequestDto CREATE_BOOKING_REQUEST_DTO =
            new CreateBookingRequestDto(
                    CHECK_IN_DATE,
                    CHECK_OUT_DATE,
                    FIRST_ACCOMMODATION_ID
            );

    public static final CreateBookingRequestDto INVALID_CREATE_BOOKING_REQUEST_DTO =
            new CreateBookingRequestDto(
                    LocalDate.now(),
                    LocalDate.now().plusDays(10),
                    INVALID_ACCOMMODATION_ID
            );

    public static final Booking FIRST_BOOKING = new Booking();

    static {
        FIRST_BOOKING.setId(FIRST_BOOKING_ID);
        FIRST_BOOKING.setCheckInDate(LocalDate.now());
        FIRST_BOOKING.setCheckOutDate(LocalDate.now().plusDays(10));
        FIRST_BOOKING.setAccommodation(FIRST_ACCOMMODATION);
        FIRST_BOOKING.setUser(FIRST_USER);
        FIRST_BOOKING.setStatus(Booking.BookingStatus.PENDING);
    }

    public static final Booking SECOND_BOOKING = new Booking();

    static {
        SECOND_BOOKING.setId(SECOND_BOOKING_ID);
        SECOND_BOOKING.setCheckInDate(LocalDate.now());
        SECOND_BOOKING.setCheckOutDate(LocalDate.now().plusDays(11));
        SECOND_BOOKING.setAccommodation(FIRST_ACCOMMODATION);
        SECOND_BOOKING.setUser(FIRST_USER);
        SECOND_BOOKING.setStatus(Booking.BookingStatus.PENDING);
    }

    public static final BookingDto BOOKING_RESPONSE_DTO = new BookingDto(
            FIRST_BOOKING_ID,
            LocalDate.now(),
            LocalDate.now().plusDays(10),
            FIRST_ACCOMMODATION_ID,
            Booking.BookingStatus.PENDING
    );

    public static final BookingDto CANCELLED_BOOKING_RESPONSE_DTO = new BookingDto(
            SECOND_BOOKING_ID,
            LocalDate.now(),
            LocalDate.now().plusDays(11),
            FIRST_ACCOMMODATION_ID,
            Booking.BookingStatus.CANCELED
    );

    public static final PageRequest PAGEABLE = PageRequest.of(0, 1);
    public static final Page<Booking> BOOKING_PAGE = new PageImpl<>(
            List.of(FIRST_BOOKING), PAGEABLE, 1
    );
    public static final BookingSearchParameters BOOKING_SEARCH_PARAMETERS =
            new BookingSearchParameters(new String[]{"PENDING"}, new String[]{"1L"});

    public static final CreatePaymentRequestDto CREATE_PAYMENT_REQUEST_DTO =
            new CreatePaymentRequestDto(
                    FIRST_BOOKING_ID
            );

    public static final Payment FIRST_PAYMENT = new Payment();

    static {
        FIRST_PAYMENT.setId(FIRST_PAYMENT_ID);
        FIRST_PAYMENT.setBooking(FIRST_BOOKING);
        FIRST_PAYMENT.setAmountToPay(PAYMENT_AMOUNT);
        FIRST_PAYMENT.setSessionId(SESSION_ID);
        try {
            FIRST_PAYMENT.setSessionUrl(new URL(SESSION_URL));
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
        FIRST_PAYMENT.setStatus(PAYMENT_STATUS);
    }

    public static final Payment SECOND_PAYMENT = new Payment();

    static {
        SECOND_PAYMENT.setId(SECOND_PAYMENT_ID);
        SECOND_PAYMENT.setBooking(SECOND_BOOKING);
        SECOND_PAYMENT.setAmountToPay(PAYMENT_AMOUNT);
        SECOND_PAYMENT.setSessionId(SECOND_SESSION_ID);
        try {
            SECOND_PAYMENT.setSessionUrl(new URL(SESSION_URL));
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
        SECOND_PAYMENT.setStatus(PAYMENT_STATUS);
    }

    public static final Payment EXPIRED_PAYMENT = new Payment();

    static {
        EXPIRED_PAYMENT.setId(EXPIRED_PAYMENT_ID);
        EXPIRED_PAYMENT.setBooking(SECOND_BOOKING);
        EXPIRED_PAYMENT.setAmountToPay(PAYMENT_AMOUNT);
        EXPIRED_PAYMENT.setSessionId(EXPIRED_SESSION_ID);
        try {
            EXPIRED_PAYMENT.setSessionUrl(new URL(SESSION_URL));
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
        EXPIRED_PAYMENT.setStatus(Payment.PaymentStatus.EXPIRED);
    }

    public static final PaymentDto PAYMENT_RESPONSE_DTO = new PaymentDto(
            FIRST_PAYMENT_ID,
            PAYMENT_AMOUNT,
            SESSION_URL,
            PENDING
    );

    public static final PaymentWithoutSessionDto PAYMENT_WITHOUT_SESSION_DTO;

    static {
        PAYMENT_WITHOUT_SESSION_DTO = new PaymentWithoutSessionDto(
                FIRST_BOOKING_ID,
                Payment.PaymentStatus.PENDING,
                PAYMENT_AMOUNT
        );
    }

    public static final PaymentWithoutSessionDto PAID_PAYMENT_WITHOUT_SESSION_DTO;

    static {
        PAID_PAYMENT_WITHOUT_SESSION_DTO = new PaymentWithoutSessionDto(
                SECOND_BOOKING_ID,
                Payment.PaymentStatus.PAID,
                BigDecimal.valueOf(200.00)
        );
    }

    public static final Session NEW_SESSION = new Session();
    public static final Session RENEWED_SESSION = new Session();
    public static final Session EXPIRED_SESSION = new Session();
    public static final SessionListParams VALID_SESSION = SessionListParams.builder()
            .setLimit(1L).build();

    static {
        // Ініціалізація NEW_SESSION
        NEW_SESSION.setId("newSessionId");
        NEW_SESSION.setStatus("active");
        // Ініціалізація RENEWED_SESSION
        RENEWED_SESSION.setId("renewedSessionId");
        RENEWED_SESSION.setStatus("active");
        // Ініціалізація EXPIRED_SESSION
        EXPIRED_SESSION.setId("expiredSessionId");
        EXPIRED_SESSION.setStatus("expired");
    }

    public static final Address INVALID_ADDRESS = new Address();

    static {
        INVALID_ADDRESS.setId(INVALID_ADDRESS_ID);
        INVALID_ADDRESS.setCountry("Canada");
        INVALID_ADDRESS.setCity("Toronto");
        INVALID_ADDRESS.setState("Ontario");
        INVALID_ADDRESS.setStreet("Bloor Street");
        INVALID_ADDRESS.setHouseNumber("123");
        INVALID_ADDRESS.setPostalCode("M6H 1M9");
    }

    public static final Specification<Booking> BOOKING_SPECIFICATION =
            (root, query, criteriaBuilder) ->
                    criteriaBuilder.equal(root.get("status"), Booking.BookingStatus.PENDING);

    public static final CreateAddressRequestDto UPDATE_ADDRESS_REQUEST_DTO =
            new CreateAddressRequestDto(
                    "USA", "Chicago", "Illinois",
                    "Cicero", "49th Ave", "60804"
            );

    public static final CreateAccommodationRequestDto UPDATE_ACCOMMODATION_REQUEST_DTO =
            new CreateAccommodationRequestDto(
                    "CONDO", UPDATE_ADDRESS_REQUEST_DTO, "Studio",
                    List.of("pool", "electricity"), BigDecimal.valueOf(150.00), 1
            );

    public static final CreateAccommodationRequestDto INVALID_CREATE_ACCOMMODATION_REQUEST_DTO =
            new CreateAccommodationRequestDto(
                    "INVALID_TYPE", CREATE_ADDRESS_REQUEST_DTO, "Studio",
                    List.of("pool", "electricity"), new BigDecimal("100.00"), 1);

    public static final UserRoleUpdateDto UPDATE_ROLE_REQUEST_DTO = new UserRoleUpdateDto(
            "ROLE_MANAGER"
    );
    public static final UserInfoUpdateDto UPDATE_USER_INFO_REQUEST_DTO = new UserInfoUpdateDto(
            "UpdatedFirstName",
            "UpdatedLastName"
    );

    public static final UserRegistrationRequestDto USER_REGISTRATION_REQUEST_DTO =
            new UserRegistrationRequestDto(
            USER_EMAIL,
            USER_PASSWORD,
            USER_FIRST_NAME,
            USER_LAST_NAME,
            USER_ROLE
    );
    public static final UserLoginRequestDto USER_LOGIN_REQUEST_DTO = new UserLoginRequestDto(
            USER_EMAIL,
            USER_PASSWORD
    );
}