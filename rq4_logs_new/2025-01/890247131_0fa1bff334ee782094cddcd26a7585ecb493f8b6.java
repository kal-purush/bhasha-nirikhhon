package org.cyberrealm.tech.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.cyberrealm.tech.util.TestConstants.CHECK_IN_DATE;
import static org.cyberrealm.tech.util.TestConstants.CHECK_OUT_DATE;
import static org.cyberrealm.tech.util.TestConstants.FIRST_ACCOMMODATION_ID;
import static org.cyberrealm.tech.util.TestConstants.FIRST_BOOKING_ID;
import static org.cyberrealm.tech.util.TestConstants.FIRST_USER_EMAIL;
import static org.cyberrealm.tech.util.TestConstants.FIRST_USER_ID;
import static org.cyberrealm.tech.util.TestConstants.NEW_CHECK_IN_DATE;
import static org.cyberrealm.tech.util.TestConstants.NEW_CHECK_OUT_DATE;
import static org.cyberrealm.tech.util.TestConstants.SECOND_USER_EMAIL;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.cyberrealm.tech.dto.booking.BookingDto;
import org.cyberrealm.tech.dto.booking.CreateBookingRequestDto;
import org.cyberrealm.tech.model.Booking;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.security.test.context.support.WithUserDetails;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
public class BookingControllerTest {
    @Autowired
    private MockMvc mockMvc;
    @Autowired
    private ObjectMapper objectMapper;

    private CreateBookingRequestDto createBookingRequestDto;
    private CreateBookingRequestDto updateBookingRequestDto;

    @BeforeEach
    void setUp() {
        createBookingRequestDto = new CreateBookingRequestDto(
                NEW_CHECK_IN_DATE,
                NEW_CHECK_OUT_DATE,
                FIRST_ACCOMMODATION_ID
        );
        updateBookingRequestDto = new CreateBookingRequestDto(
                CHECK_IN_DATE,
                CHECK_OUT_DATE,
                FIRST_ACCOMMODATION_ID
        );
    }

    @Test
    @DisplayName("Verify creation of new booking")
    @WithUserDetails(value = FIRST_USER_EMAIL)
    @Sql(scripts = {
            "classpath:database/roles/add-roles.sql",
            "classpath:database/addresses/add-addresses.sql",
            "classpath:database/accommodations/add-accommodations.sql",
            "classpath:database/users/add-users.sql",
            "classpath:database/users_roles/add-users_roles.sql",
    }, executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD)
    @Sql(scripts = {
            "classpath:database/bookings/remove-bookings.sql",
            "classpath:database/accommodations/remove-accommodations.sql",
            "classpath:database/users/remove-users.sql",
            "classpath:database/addresses/remove-addresses.sql",
            "classpath:database/roles/remove-roles.sql",
            "classpath:database/users_roles/remove-users_roles.sql"
    }, executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    void createBooking_withValidInput_returnsCreatedBooking() throws Exception {
        // When
        MvcResult mvcResult = mockMvc.perform(post("/bookings")
                        .with(csrf())
                        .contentType("application/json")
                        .content(objectMapper.writeValueAsString(createBookingRequestDto)))
                .andExpect(status().isCreated())
                .andReturn();
        // Then
        String jsonResponse = mvcResult.getResponse().getContentAsString();
        BookingDto responseDto = objectMapper.readValue(jsonResponse, BookingDto.class);
        assertThat(responseDto).isNotNull();
        assertThat(responseDto.accommodationId())
                .isEqualTo(createBookingRequestDto.accommodationId());
    }

    @Test
    @DisplayName("Get all bookings by user ID and status")
    @WithUserDetails(value = SECOND_USER_EMAIL)
    @Sql(scripts = {
            "classpath:database/roles/add-roles.sql",
            "classpath:database/addresses/add-addresses.sql",
            "classpath:database/accommodations/add-accommodations.sql",
            "classpath:database/users/add-users.sql",
            "classpath:database/users_roles/add-users_roles.sql",
            "classpath:database/bookings/add-bookings.sql"
    }, executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD)
    @Sql(scripts = {
            "classpath:database/bookings/remove-bookings.sql",
            "classpath:database/accommodations/remove-accommodations.sql",
            "classpath:database/users/remove-users.sql",
            "classpath:database/addresses/remove-addresses.sql",
            "classpath:database/roles/remove-roles.sql",
            "classpath:database/users_roles/remove-users_roles.sql"
    }, executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    void searchBookings_withValidParameters_returnsBookings() throws Exception {
        // When
        MvcResult mvcResult = mockMvc.perform(get("/bookings")
                        .param("userId", String.valueOf(FIRST_USER_ID))
                        .param("status", String.valueOf(Booking.BookingStatus.PENDING)))
                .andExpect(status().isOk())
                .andReturn();
        // Then
        String jsonResponse = mvcResult.getResponse().getContentAsString();
        BookingDto[] responseDtos = objectMapper.readValue(jsonResponse, BookingDto[].class);
        assertThat(responseDtos).isNotEmpty();
    }

    @Test
    @DisplayName("Get user bookings")
    @WithUserDetails(value = FIRST_USER_EMAIL)
    @Sql(scripts = {
            "classpath:database/roles/add-roles.sql",
            "classpath:database/addresses/add-addresses.sql",
            "classpath:database/accommodations/add-accommodations.sql",
            "classpath:database/users/add-users.sql",
            "classpath:database/users_roles/add-users_roles.sql",
            "classpath:database/bookings/add-bookings.sql"
    }, executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD)
    @Sql(scripts = {
            "classpath:database/bookings/remove-bookings.sql",
            "classpath:database/accommodations/remove-accommodations.sql",
            "classpath:database/users/remove-users.sql",
            "classpath:database/addresses/remove-addresses.sql",
            "classpath:database/roles/remove-roles.sql",
            "classpath:database/users_roles/remove-users_roles.sql"
    }, executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    void getUserBookings_withValidRequest_returnsBookings() throws Exception {
        // When
        MvcResult mvcResult = mockMvc.perform(get("/bookings/my"))
                .andExpect(status().isOk())
                .andReturn();
        // Then
        String jsonResponse = mvcResult.getResponse().getContentAsString();
        BookingDto[] responseDtos = objectMapper.readValue(jsonResponse, BookingDto[].class);
        assertThat(responseDtos).isNotEmpty();
    }

    @Test
    @DisplayName("Get booking by id")
    @WithUserDetails(value = SECOND_USER_EMAIL)
    @Sql(scripts = {
            "classpath:database/roles/add-roles.sql",
            "classpath:database/addresses/add-addresses.sql",
            "classpath:database/accommodations/add-accommodations.sql",
            "classpath:database/users/add-users.sql",
            "classpath:database/users_roles/add-users_roles.sql",
            "classpath:database/bookings/add-bookings.sql"
    }, executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD)
    @Sql(scripts = {
            "classpath:database/bookings/remove-bookings.sql",
            "classpath:database/accommodations/remove-accommodations.sql",
            "classpath:database/users/remove-users.sql",
            "classpath:database/addresses/remove-addresses.sql",
            "classpath:database/roles/remove-roles.sql",
            "classpath:database/users_roles/remove-users_roles.sql"
    }, executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    void getBookingById_withValidId_returnsBooking() throws Exception {
        // When
        MvcResult mvcResult = mockMvc.perform(get("/bookings/{id}", FIRST_BOOKING_ID))
                .andExpect(status().isOk())
                .andReturn();
        // Then
        String jsonResponse = mvcResult.getResponse().getContentAsString();
        BookingDto responseDto = objectMapper.readValue(jsonResponse, BookingDto.class);
        assertThat(responseDto).isNotNull();
        assertThat(responseDto.id()).isEqualTo(FIRST_BOOKING_ID);
    }

    @Test
    @DisplayName("Update booking by id")
    @WithUserDetails(value = SECOND_USER_EMAIL)
    @Sql(scripts = {
            "classpath:database/roles/add-roles.sql",
            "classpath:database/addresses/add-addresses.sql",
            "classpath:database/accommodations/add-accommodations.sql",
            "classpath:database/users/add-users.sql",
            "classpath:database/users_roles/add-users_roles.sql",
            "classpath:database/bookings/add-bookings.sql"
    }, executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD)
    @Sql(scripts = {
            "classpath:database/bookings/remove-bookings.sql",
            "classpath:database/accommodations/remove-accommodations.sql",
            "classpath:database/users/remove-users.sql",
            "classpath:database/addresses/remove-addresses.sql",
            "classpath:database/roles/remove-roles.sql",
            "classpath:database/users_roles/remove-users_roles.sql"
    }, executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    void updateBooking_withValidId_returnsUpdatedBooking() throws Exception {
        // When
        MvcResult mvcResult = mockMvc.perform(put("/bookings/{id}", FIRST_BOOKING_ID)
                        .with(csrf())
                        .contentType("application/json")
                        .content(objectMapper.writeValueAsString(updateBookingRequestDto)))
                .andExpect(status().isOk())
                .andReturn();
        // Then
        String jsonResponse = mvcResult.getResponse().getContentAsString();
        BookingDto responseDto = objectMapper.readValue(jsonResponse, BookingDto.class);
        assertThat(responseDto).isNotNull();
        assertThat(responseDto.accommodationId())
                .isEqualTo(updateBookingRequestDto.accommodationId());
    }

    @Test
    @DisplayName("Cancel booking by id")
    @WithUserDetails(value = SECOND_USER_EMAIL)
    @Sql(scripts = {
            "classpath:database/roles/add-roles.sql",
            "classpath:database/addresses/add-addresses.sql",
            "classpath:database/accommodations/add-accommodations.sql",
            "classpath:database/users/add-users.sql",
            "classpath:database/users_roles/add-users_roles.sql",
            "classpath:database/bookings/add-bookings.sql"
    }, executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD)
    @Sql(scripts = {
            "classpath:database/bookings/remove-bookings.sql",
            "classpath:database/accommodations/remove-accommodations.sql",
            "classpath:database/users/remove-users.sql",
            "classpath:database/addresses/remove-addresses.sql",
            "classpath:database/roles/remove-roles.sql",
            "classpath:database/users_roles/remove-users_roles.sql"
    }, executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    void cancelBooking_withValidId_deletesBooking() throws Exception {
        // When
        MvcResult mvcResult = mockMvc.perform(delete("/bookings/{id}", FIRST_BOOKING_ID)
                        .with(csrf()))
                .andExpect(status().isNoContent())
                .andReturn();
        // Then
        String jsonResponse = mvcResult.getResponse().getContentAsString();
        BookingDto responseDto = objectMapper.readValue(jsonResponse, BookingDto.class);
        assertThat(responseDto).isNotNull();
        assertThat(responseDto.status()).isEqualTo(Booking.BookingStatus.CANCELED);
    }
}