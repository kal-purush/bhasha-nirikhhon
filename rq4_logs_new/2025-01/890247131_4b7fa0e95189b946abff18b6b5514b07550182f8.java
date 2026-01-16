package org.cyberrealm.tech.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.cyberrealm.tech.util.TestConstants.ACCOMMODATION_TYPE_HOUSE;
import static org.cyberrealm.tech.util.TestConstants.DAILY_RATE_23;
import static org.cyberrealm.tech.util.TestConstants.ELECTRICITY;
import static org.cyberrealm.tech.util.TestConstants.FIRST_ACCOMMODATION_ID;
import static org.cyberrealm.tech.util.TestConstants.FIRST_AVAILABILITY;
import static org.cyberrealm.tech.util.TestConstants.INVALID_ACCOMMODATION_ID;
import static org.cyberrealm.tech.util.TestConstants.POOL;
import static org.cyberrealm.tech.util.TestConstants.STUDIO;
import static org.cyberrealm.tech.util.TestConstants.WIFI;
import static org.cyberrealm.tech.util.TestUtil.FIRST_ADDRESS;
import static org.cyberrealm.tech.util.TestUtil.INVALID_CREATE_ACCOMMODATION_REQUEST_DTO;
import static org.cyberrealm.tech.util.TestUtil.UPDATE_ACCOMMODATION_REQUEST_DTO;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import org.cyberrealm.tech.dto.accommodation.AccommodationDto;
import org.cyberrealm.tech.dto.accommodation.CreateAccommodationRequestDto;
import org.cyberrealm.tech.dto.address.CreateAddressRequestDto;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
public class AccommodationControllerTest {
    @Autowired
    private MockMvc mockMvc;
    @Autowired
    private ObjectMapper objectMapper;
    private CreateAccommodationRequestDto requestDto;
    private CreateAddressRequestDto addressRequestDto;
    private List<String> amenities;

    @BeforeEach
    void setUp() {
        amenities = List.of(POOL, ELECTRICITY, WIFI);
        addressRequestDto = new CreateAddressRequestDto(
                FIRST_ADDRESS.getCountry(),
                FIRST_ADDRESS.getCity(),
                FIRST_ADDRESS.getState(),
                FIRST_ADDRESS.getStreet(),
                FIRST_ADDRESS.getHouseNumber(),
                FIRST_ADDRESS.getPostalCode()
        );
        requestDto = new CreateAccommodationRequestDto(
                ACCOMMODATION_TYPE_HOUSE,
                addressRequestDto,
                STUDIO,
                amenities,
                DAILY_RATE_23,
                FIRST_AVAILABILITY
        );
    }

    @Test
    @DisplayName("Verify creation of new accommodation")
    @WithMockUser(roles = "MANAGER")
    @Sql(scripts = {
            "classpath:database/accommodations/remove-accommodations.sql",
            "classpath:database/addresses/remove-addresses.sql"
    }, executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD)
    @Sql(scripts = {
            "classpath:database/accommodations/remove-accommodations.sql",
            "classpath:database/addresses/remove-addresses.sql"
    }, executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    void createAccommodation_withValidInput_returnsCreatedAccommodation() throws Exception {
        // When
        MvcResult mvcResult = mockMvc.perform(post("/accommodations")
                        .with(csrf())
                        .contentType("application/json")
                        .content(objectMapper.writeValueAsString(requestDto)))
                .andExpect(status().isCreated())
                .andReturn();
        // Then
        String jsonResponse = mvcResult.getResponse().getContentAsString();
        AccommodationDto responseDto = objectMapper.readValue(jsonResponse, AccommodationDto.class);
        assertThat(responseDto).isNotNull();
        assertThat(responseDto.type()).isEqualTo(requestDto.type());
    }

    @Test
    @DisplayName("Get all accommodations")
    @WithMockUser(roles = {"MANAGER", "CUSTOMER"})
    @Sql(scripts = {
            "classpath:database/addresses/add-addresses.sql",
            "classpath:database/accommodations/add-accommodations.sql"
    }, executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD)
    @Sql(scripts = {
            "classpath:database/accommodations/remove-accommodations.sql",
            "classpath:database/addresses/remove-addresses.sql"
    }, executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    void getAllAccommodations_withValidRequest_returnsAccommodations() throws Exception {
        // When
        MvcResult mvcResult = mockMvc.perform(get("/accommodations"))
                .andExpect(status().isOk())
                .andReturn();
        // Then
        String jsonResponse = mvcResult.getResponse().getContentAsString();
        AccommodationDto[] responseDtos = objectMapper.readValue(jsonResponse,
                AccommodationDto[].class);
        assertThat(responseDtos).isNotEmpty();
    }

    @Test
    @DisplayName("Get accommodation by id")
    @WithMockUser(roles = {"MANAGER", "CUSTOMER"})
    @Sql(scripts = {
            "classpath:database/addresses/add-addresses.sql",
            "classpath:database/accommodations/add-accommodations.sql"
    }, executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD)
    @Sql(scripts = {
            "classpath:database/accommodations/remove-accommodations.sql",
            "classpath:database/addresses/remove-addresses.sql"
    }, executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    void getAccommodationById_withValidId_returnsAccommodation() throws Exception {
        // When
        MvcResult mvcResult = mockMvc.perform(get("/accommodations/{id}",
                        FIRST_ACCOMMODATION_ID))
                .andExpect(status().isOk())
                .andReturn();
        // Then
        String jsonResponse = mvcResult.getResponse().getContentAsString();
        AccommodationDto responseDto = objectMapper.readValue(jsonResponse, AccommodationDto.class);
        assertThat(responseDto).isNotNull();
        assertThat(responseDto.id()).isEqualTo(FIRST_ACCOMMODATION_ID);
    }

    @Test
    @DisplayName("Get accommodation by invalid id")
    @WithMockUser(roles = {"MANAGER", "CUSTOMER"})
    @Sql(scripts = {
            "classpath:database/addresses/add-addresses.sql",
            "classpath:database/accommodations/add-accommodations.sql"
    }, executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD)
    @Sql(scripts = {
            "classpath:database/accommodations/remove-accommodations.sql",
            "classpath:database/addresses/remove-addresses.sql"
    }, executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    void getAccommodationById_withInvalidId_returnsNotFound() throws Exception {
        // When
        mockMvc.perform(get("/accommodations/{id}", INVALID_ACCOMMODATION_ID))
                .andExpect(status().isNotFound());
    }

    @Test
    @DisplayName("Update accommodation by id")
    @WithMockUser(roles = "MANAGER")
    @Sql(scripts = {
            "classpath:database/addresses/add-addresses.sql",
            "classpath:database/accommodations/add-accommodations.sql"
    }, executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD)
    @Sql(scripts = {
            "classpath:database/accommodations/remove-accommodations.sql",
            "classpath:database/addresses/remove-addresses.sql"
    }, executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    void updateAccommodation_withValidId_returnsUpdatedAccommodation() throws Exception {
        // When
        MvcResult mvcResult = mockMvc.perform(put("/accommodations/{id}",
                        FIRST_ACCOMMODATION_ID)
                        .with(csrf())
                        .contentType("application/json")
                        .content(objectMapper.writeValueAsString(UPDATE_ACCOMMODATION_REQUEST_DTO)))
                .andExpect(status().isOk())
                .andReturn();
        // Then
        String jsonResponse = mvcResult.getResponse().getContentAsString();
        AccommodationDto responseDto = objectMapper.readValue(jsonResponse, AccommodationDto.class);
        assertThat(responseDto).isNotNull();
        assertThat(responseDto.type()).isEqualTo(UPDATE_ACCOMMODATION_REQUEST_DTO.type());
    }

    @Test
    @DisplayName("Update accommodation by invalid id")
    @WithMockUser(roles = "MANAGER")
    @Sql(scripts = {
            "classpath:database/addresses/add-addresses.sql",
            "classpath:database/accommodations/add-accommodations.sql"
    }, executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD)
    @Sql(scripts = {
            "classpath:database/accommodations/remove-accommodations.sql",
            "classpath:database/addresses/remove-addresses.sql"
    }, executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    void updateAccommodation_withInvalidId_returnsNotFound() throws Exception {
        // When
        mockMvc.perform(put("/accommodations/{id}", INVALID_ACCOMMODATION_ID)
                        .with(csrf())
                        .contentType("application/json")
                        .content(objectMapper.writeValueAsString(UPDATE_ACCOMMODATION_REQUEST_DTO)))
                .andExpect(status().isNotFound());
    }

    @Test
    @DisplayName("Delete accommodation by id")
    @WithMockUser(roles = "MANAGER")
    @Sql(scripts = {
            "classpath:database/addresses/add-addresses.sql",
            "classpath:database/accommodations/add-accommodations.sql"
    }, executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD)
    @Sql(scripts = {
            "classpath:database/accommodations/remove-accommodations.sql",
            "classpath:database/addresses/remove-addresses.sql"
    }, executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    void deleteAccommodation_withValidId_deletesAccommodation() throws Exception {
        // When
        mockMvc.perform(delete("/accommodations/{id}", FIRST_ACCOMMODATION_ID)
                        .with(csrf()))
                .andExpect(status().isNoContent());
        // Then
        mockMvc.perform(get("/accommodations/{id}", FIRST_ACCOMMODATION_ID))
                .andExpect(status().isNotFound());
    }

    @Test
    @DisplayName("Create accommodation with invalid data")
    @WithMockUser(roles = "MANAGER")
    @Sql(scripts = {
            "classpath:database/addresses/add-addresses.sql",
            "classpath:database/accommodations/add-accommodations.sql"
    }, executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD)
    @Sql(scripts = {
            "classpath:database/accommodations/remove-accommodations.sql",
            "classpath:database/addresses/remove-addresses.sql"
    }, executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    void createAccommodation_withInvalidData_returnsBadRequest() throws Exception {
        // When
        MvcResult mvcResult = mockMvc.perform(post("/accommodations")
                        .with(csrf())
                        .contentType("application/json")
                        .content(objectMapper.writeValueAsString(
                                INVALID_CREATE_ACCOMMODATION_REQUEST_DTO
                        )))
                .andExpect(status().isBadRequest())
                .andReturn();
        // Then
        String jsonResponse = mvcResult.getResponse().getContentAsString();
        assertThat(jsonResponse).contains("Invalid type value");
    }
}

