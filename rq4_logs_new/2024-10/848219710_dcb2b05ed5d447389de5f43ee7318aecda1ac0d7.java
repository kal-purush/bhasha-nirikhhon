package com.example.spring.controller;

import static org.junit.Assert.assertNotNull;
import static org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.example.spring.dto.ShoppingCartDto;
import com.example.spring.security.JwtUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Sql(
        scripts = {
                "classpath:database/shopping.carts/clear-cart.sql",
                "classpath:database/cart.items/clear-cart-items.sql",
                "classpath:database/books/clear-books.sql",
                "classpath:database/shopping.carts/add-cart.sql",
                "classpath:database/roles/add-role.sql",
                "classpath:database/users-roles/add-user-role.sql",
                "classpath:database/books/add-book.sql",
                "classpath:database/cart.items/add-cart-items.sql",
        },
        executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD
)
@Sql(
        scripts = {
                "classpath:database/users-roles/clear-users-roles.sql",
                "classpath:database/roles/clear-role.sql",
                "classpath:database/cart.items/clear-cart-items.sql",
                "classpath:database/shopping.carts/clear-cart.sql",
                "classpath:database/books/clear-books.sql"
        },
        executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD
)
public class ShoppingCartControllerTest {
    protected static MockMvc mockMvc;
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private JwtUtil jwtUtil;

    @BeforeAll
    static void beforeAll(@Autowired WebApplicationContext applicationContext) {
        mockMvc = MockMvcBuilders
                .webAppContextSetup(applicationContext)
                .apply(springSecurity())
                .build();
    }

    @Test
    @DisplayName("Get the shopping cart for a user")
    void getCart_ValidUser_Success() throws Exception {
        // Given
        String jwtToken = generateJwtTokenForTestUser();

        // When
        MvcResult result = mockMvc.perform(get("/cart")
                        .header("Authorization", "Bearer " + jwtToken)
                        .contentType(MediaType.APPLICATION_JSON_VALUE))
                .andExpect(status().isOk())
                .andReturn();

        // Then
        ShoppingCartDto cartDto = objectMapper.readValue(
                result.getResponse().getContentAsString(), ShoppingCartDto.class);
        assertNotNull(cartDto);
    }

    private String generateJwtTokenForTestUser() {
        return jwtUtil.generateToken("testUser@test.com");
    }

    @Test
    @WithMockUser(username = "admin", roles = {"ADMIN"})
    @DisplayName("Delete an item from the shopping cart")
    void deleteCartItem_ValidId_Success() throws Exception {
        // Given
        Long cartItemId = 1L;

        // When
        mockMvc.perform(delete("/cart/items/{id}", cartItemId)
                        .contentType(MediaType.APPLICATION_JSON_VALUE))
                .andExpect(status().isNoContent());
    }
}
