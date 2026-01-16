package com.jaimayal.journey;

import com.jaimayal.auth.AuthenticationRequest;
import com.jaimayal.auth.AuthenticationResponse;
import com.jaimayal.config.JwtService;
import com.jaimayal.customer.CustomerRegistrationRequest;
import com.jaimayal.utils.CustomEntityFaker;
import com.jaimayal.utils.IntegrationUtils;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpHeaders;
import org.springframework.test.web.reactive.server.EntityExchangeResult;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class LoginIntegrationTest {

    private static final CustomEntityFaker ENTITY_FAKER = new CustomEntityFaker();
    private static final IntegrationUtils INTEGRATION_UTILS = new IntegrationUtils();
    private static final String CUSTOMERS_API_URI = "api/v1/customers";
    private static final String AUTH_API_URI = "api/v1/auth";
    
    @Autowired
    private WebTestClient webTestClient;
    @Autowired
    private JwtService jwtService;
    
    @Test
    public void canLoginCustomer() {
        // Register a new customer
        CustomerRegistrationRequest registrationRequest = ENTITY_FAKER.getCustomerRegistrationRequest();
        INTEGRATION_UTILS.registerNewCustomerAndGetToken(registrationRequest, webTestClient, CUSTOMERS_API_URI);
        
        // Create an AuthenticationRequest
        AuthenticationRequest authRequest = new AuthenticationRequest(
                registrationRequest.email(),
                registrationRequest.password()
        );
        
        // Send a POST request to "api/v1/auth/login"
        EntityExchangeResult<AuthenticationResponse> result = webTestClient.post()
                .uri(AUTH_API_URI + "/login")
                .body(Mono.just(authRequest), AuthenticationRequest.class)
                .exchange()
                .expectStatus()
                .isOk()
                .expectBody(new ParameterizedTypeReference<AuthenticationResponse>() {
                })
                .returnResult();
        
        AuthenticationResponse response = result.getResponseBody();
        String token = result.getResponseHeaders().getFirst(HttpHeaders.AUTHORIZATION);
        
        assertThat(response).isNotNull();
        assertThat(response.token()).isNotEmpty();
        assertThat(token).isEqualTo(response.token());
        assertThat(jwtService.isValid(token, registrationRequest.email())).isTrue();
    }
}