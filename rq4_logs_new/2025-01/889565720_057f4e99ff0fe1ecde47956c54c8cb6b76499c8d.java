package com.app.bookstore.backend.controller;

import com.app.bookstore.backend.DTO.BookRequestDTO;
import com.app.bookstore.backend.DTO.JsonResponseDTO;
import com.app.bookstore.backend.config.SecurityConfig;
import com.app.bookstore.backend.mapper.UserMapper;
import com.app.bookstore.backend.model.User;
import com.app.bookstore.backend.service.BookService;
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
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

@WebMvcTest(controllers = BookController.class)
@AutoConfigureMockMvc(addFilters = false)
@Import(SecurityConfig.class)
@ExtendWith(MockitoExtension.class)
class BookControllerTest
{
    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private BookService bookService;

    @MockBean
    private JWTService jwtService;

    @MockBean
    private UserMapper userMapper;

    private UserDetails userDetails;
    private User user;

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
                .role("ADMIN")
                .userId(1L)
                .build();

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
    }

    @Test
    public void BookController_AddBook_MustAddBook() throws Exception
    {
        String token="Bearer token";
        BookRequestDTO requestDTO=BookRequestDTO.builder()
                .name("Atomic Habits")
                .price(199.09)
                .bookLogo("URL")
                .quantity(78)
                .author("James Clear")
                .description("Improving by 1% daily")
                .build();

        JsonResponseDTO responseDTO=new JsonResponseDTO(true,"Book added successfully", List.of(requestDTO));

        given(bookService.addBook(ArgumentMatchers.any())).willReturn(responseDTO);
        given(jwtService.validateToken(ArgumentMatchers.any(),ArgumentMatchers.any())).willReturn(true);
        given(userMapper.validateUserToken(ArgumentMatchers.any())).willReturn(userDetails);

        mockMvc.perform(post("/books/addBook")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(requestDTO))
                .characterEncoding(StandardCharsets.UTF_8)
                .header("Authorization",token))
                .andDo(MockMvcResultHandlers.print())
                .andExpect(MockMvcResultMatchers.status().isCreated())
                .andExpect(MockMvcResultMatchers.jsonPath("$.message", CoreMatchers.is(responseDTO.getMessage())));
    }
}