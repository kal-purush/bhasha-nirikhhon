package com.app.bookstore.backend.controller;

import com.app.bookstore.backend.DTO.CartRequestDTO;
import com.app.bookstore.backend.DTO.CartResponseDTO;
import com.app.bookstore.backend.DTO.JsonResponseDTO;
import com.app.bookstore.backend.config.SecurityConfig;
import com.app.bookstore.backend.mapper.UserMapper;
import com.app.bookstore.backend.model.User;
import com.app.bookstore.backend.service.CartService;
import com.app.bookstore.backend.serviceimpl.JWTService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.result.MockMvcResultHandlers;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.BDDMockito.given;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;

@WebMvcTest(controllers = CartController.class)
@AutoConfigureMockMvc(addFilters = false)
@ExtendWith(MockitoExtension.class)
@Import(SecurityConfig.class)
class CartControllerTest
{
    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private CartService cartService;

    @MockBean
    private JWTService jwtService;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private UserMapper userMapper;

    private User admin;
    private User user;
    private UserDetails userDetails;
    private UserDetails adminDetails;
    private CartRequestDTO cartRequestDTO;
    private CartResponseDTO cartResponseDTO;

    @BeforeEach
    public void init()
    {
        user=User.builder()
                .firstName("Sai")
                .lastName("Chandu")
                .dob(LocalDate.of(2002,8,24))
                .email("saichandu090@gmail.com")
                .password("saichandu@090")
                .registeredDate(LocalDate.now())
                .updatedDate(LocalDate.now())
                .role("USER")
                .userId(1L)
                .build();

        admin=User.builder()
                .firstName("Sai")
                .lastName("Chandu")
                .dob(LocalDate.of(2002,8,24))
                .email("saichandu090@gmail.com")
                .password("saichandu@090")
                .registeredDate(LocalDate.now())
                .updatedDate(LocalDate.now())
                .role("ADMIN")
                .userId(2L)
                .build();

        adminDetails=new UserDetails() {
            @Override
            public Collection<? extends GrantedAuthority> getAuthorities()
            {
                return Collections.singleton(new SimpleGrantedAuthority(admin.getRole()));
            }

            @Override
            public String getPassword() {
                return admin.getPassword();
            }

            @Override
            public String getUsername() {
                return admin.getEmail();
            }
        };

        userDetails=new UserDetails() {
            @Override
            public Collection<? extends GrantedAuthority> getAuthorities()
            {
                return Collections.singleton(new SimpleGrantedAuthority(user.getRole()));
            }

            @Override
            public String getPassword() {
                return user.getPassword();
            }

            @Override
            public String getUsername() {
                return user.getEmail();
            }
        };

        cartRequestDTO=CartRequestDTO.builder()
                .bookId(1L)
                .quantity(1).build();

        cartResponseDTO=CartResponseDTO.builder()
                .cartId(1L)
                .bookId(1L)
                .bookName("Atomic Habits")
                .quantity(1)
                .totalPrice(199.87)
                .bookLogo("URL")
                .userId(user.getUserId()).build();
    }

    @Test
    public void CartController_AddToCart_MustAddBookToCart() throws Exception
    {
        String token="Bearer token";
        JsonResponseDTO responseDTO=new JsonResponseDTO(true,"Book added to cart successfully", List.of(cartResponseDTO));

        given(cartService.addToCart(ArgumentMatchers.any(),ArgumentMatchers.any())).willReturn(responseDTO);
        given(jwtService.validateToken(ArgumentMatchers.any(),ArgumentMatchers.any())).willReturn(true);
        given(userMapper.validateUserToken(ArgumentMatchers.any())).willReturn(userDetails);

        mockMvc.perform(post("/cart/addToCart")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(cartRequestDTO))
                        .characterEncoding(StandardCharsets.UTF_8)
                        .header("Authorization",token))
                .andDo(MockMvcResultHandlers.print())
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.message", CoreMatchers.is(responseDTO.getMessage())));
    }

    @Test
    public void CartController_GetCart_MustReturnAllUserCarts() throws Exception
    {
        String token="Bearer token";
        JsonResponseDTO responseDTO=new JsonResponseDTO(true,"All User carts fetched successfully", List.of(cartResponseDTO));

        given(cartService.getUserCarts(ArgumentMatchers.any())).willReturn(responseDTO);
        given(jwtService.validateToken(ArgumentMatchers.any(),ArgumentMatchers.any())).willReturn(true);
        given(userMapper.validateUserToken(ArgumentMatchers.any())).willReturn(userDetails);

        mockMvc.perform(get("/cart/getCart")
                        .contentType(MediaType.APPLICATION_JSON)
                        .characterEncoding(StandardCharsets.UTF_8)
                        .header("Authorization",token))
                .andDo(MockMvcResultHandlers.print())
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.message", CoreMatchers.is(responseDTO.getMessage())));
    }

    @Test
    public void CartController_GetCartById_MustReturnCart() throws Exception
    {
        String token="Bearer token";
        JsonResponseDTO responseDTO=new JsonResponseDTO(true,"Cart fetched successfully", List.of(cartResponseDTO));

        given(cartService.getUserCartById(ArgumentMatchers.any(),ArgumentMatchers.anyLong())).willReturn(responseDTO);
        given(jwtService.validateToken(ArgumentMatchers.any(),ArgumentMatchers.any())).willReturn(true);
        given(userMapper.validateUserToken(ArgumentMatchers.any())).willReturn(userDetails);

        mockMvc.perform(get("/cart/getCartById/{id}",cartResponseDTO.getCartId())
                        .contentType(MediaType.APPLICATION_JSON)
                        .characterEncoding(StandardCharsets.UTF_8)
                        .header("Authorization",token))
                .andDo(MockMvcResultHandlers.print())
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.message", CoreMatchers.is(responseDTO.getMessage())));
    }

    @Test
    public void CartController_RemoveFromCart_MustRemoveCart() throws Exception
    {
        String token="Bearer token";
        JsonResponseDTO responseDTO=new JsonResponseDTO(true,"Cart Removed successfully",null);

        given(cartService.removeFromCart(ArgumentMatchers.any(),ArgumentMatchers.anyLong())).willReturn(responseDTO);
        given(jwtService.validateToken(ArgumentMatchers.any(),ArgumentMatchers.any())).willReturn(true);
        given(userMapper.validateUserToken(ArgumentMatchers.any())).willReturn(userDetails);

        mockMvc.perform(delete("/cart/removeFromCart/{id}",cartResponseDTO.getCartId())
                        .contentType(MediaType.APPLICATION_JSON)
                        .characterEncoding(StandardCharsets.UTF_8)
                        .header("Authorization",token))
                .andDo(MockMvcResultHandlers.print())
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.message", CoreMatchers.is(responseDTO.getMessage())));
    }

    @Test
    public void CartController_ClearCart_MustRemoveAllUserCarts() throws Exception
    {
        String token="Bearer token";
        JsonResponseDTO responseDTO=new JsonResponseDTO(true,"All User Carts Removed successfully",null);

        given(cartService.clearCart(ArgumentMatchers.any())).willReturn(responseDTO);
        given(jwtService.validateToken(ArgumentMatchers.any(),ArgumentMatchers.any())).willReturn(true);
        given(userMapper.validateUserToken(ArgumentMatchers.any())).willReturn(userDetails);

        mockMvc.perform(delete("/cart/clearCart")
                        .contentType(MediaType.APPLICATION_JSON)
                        .characterEncoding(StandardCharsets.UTF_8)
                        .header("Authorization",token))
                .andDo(MockMvcResultHandlers.print())
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.message", CoreMatchers.is(responseDTO.getMessage())));
    }

    @Test
    public void CartController_GetAllCarts_MustReturnAllCarts() throws Exception
    {
        String token="Bearer token";
        JsonResponseDTO responseDTO=new JsonResponseDTO(true,"All User Carts Removed successfully",List.of(cartResponseDTO));

        given(cartService.getAllCarts()).willReturn(responseDTO);
        given(jwtService.validateToken(ArgumentMatchers.any(),ArgumentMatchers.any())).willReturn(true);
        given(userMapper.validateUserToken(ArgumentMatchers.any())).willReturn(adminDetails);

        mockMvc.perform(get("/cart/getAllCarts")
                        .contentType(MediaType.APPLICATION_JSON)
                        .characterEncoding(StandardCharsets.UTF_8)
                        .header("Authorization",token))
                .andDo(MockMvcResultHandlers.print())
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.message", CoreMatchers.is(responseDTO.getMessage())));
    }
}