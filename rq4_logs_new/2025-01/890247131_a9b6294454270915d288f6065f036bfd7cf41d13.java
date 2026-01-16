package org.cyberrealm.tech.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.cyberrealm.tech.util.TestConstants.FIRST_USER_EMAIL;
import static org.cyberrealm.tech.util.TestConstants.USER_EMAIL;
import static org.cyberrealm.tech.util.TestConstants.USER_FIRST_NAME;
import static org.cyberrealm.tech.util.TestConstants.USER_LAST_NAME;
import static org.cyberrealm.tech.util.TestConstants.USER_PASSWORD;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.cyberrealm.tech.dto.user.UserLoginRequestDto;
import org.cyberrealm.tech.dto.user.UserLoginResponseDto;
import org.cyberrealm.tech.dto.user.UserRegistrationRequestDto;
import org.cyberrealm.tech.dto.user.UserResponseDto;
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
public class AuthenticationControllerTest {
    @Autowired
    private MockMvc mockMvc;
    @Autowired
    private ObjectMapper objectMapper;

    private UserRegistrationRequestDto userRegistrationRequestDto;
    private UserLoginRequestDto userLoginRequestDto;

    @BeforeEach
    void setUp() {
        userRegistrationRequestDto = new UserRegistrationRequestDto(
                USER_EMAIL,
                USER_PASSWORD,
                USER_PASSWORD,
                USER_FIRST_NAME,
                USER_LAST_NAME
        );

        userLoginRequestDto = new UserLoginRequestDto(
                FIRST_USER_EMAIL,
                USER_PASSWORD
        );
    }

    @Test
    @DisplayName("Register a new user")
    @WithMockUser(username = FIRST_USER_EMAIL, roles = "CUSTOMER")
    @Sql(scripts = {
            "classpath:database/roles/add-roles.sql"
    }, executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD)
    @Sql(scripts = {
            "classpath:database/users_roles/remove-users_roles.sql",
            "classpath:database/roles/remove-roles.sql",
            "classpath:database/users/remove-users.sql"
    }, executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    void register_withValidInput_createsNewUser() throws Exception {
        // When
        MvcResult mvcResult = mockMvc.perform(post("/auth/registration")
                        .contentType("application/json")
                        .content(objectMapper.writeValueAsString(userRegistrationRequestDto)))
                .andExpect(status().isCreated())
                .andReturn();
        // Then
        String jsonResponse = mvcResult.getResponse().getContentAsString();
        UserResponseDto responseDto = objectMapper.readValue(jsonResponse, UserResponseDto.class);
        assertThat(responseDto).isNotNull();
        assertThat(responseDto.email()).isEqualTo(userRegistrationRequestDto.email());
    }

    @Test
    @DisplayName("Authenticate user")
    @WithMockUser(username = FIRST_USER_EMAIL, roles = "CUSTOMER")
    @Sql(scripts = {
            "classpath:database/roles/add-roles.sql",
            "classpath:database/users/add-users.sql",
            "classpath:database/users_roles/add-users_roles.sql"
    }, executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD)
    @Sql(scripts = {
            "classpath:database/users_roles/remove-users_roles.sql",
            "classpath:database/roles/remove-roles.sql",
            "classpath:database/users/remove-users.sql"
    }, executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    void login_withValidInput_returnsToken() throws Exception {
        // When
        MvcResult mvcResult = mockMvc.perform(post("/auth/login")
                        .contentType("application/json")
                        .content(objectMapper.writeValueAsString(userLoginRequestDto)))
                .andExpect(status().isOk())
                .andReturn();
        // Then
        String jsonResponse = mvcResult.getResponse().getContentAsString();
        System.out.println(jsonResponse);
        UserLoginResponseDto responseDto = objectMapper.readValue(jsonResponse,
                UserLoginResponseDto.class);
        assertThat(responseDto).isNotNull();
        assertThat(responseDto.token()).isNotEmpty();
    }
}