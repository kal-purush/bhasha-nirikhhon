package com.app.bookstore.backend.controller;

import com.app.bookstore.backend.DTO.JsonResponseDTO;
import com.app.bookstore.backend.DTO.WishListDTO;
import com.app.bookstore.backend.config.SecurityConfig;
import com.app.bookstore.backend.mapper.UserMapper;
import com.app.bookstore.backend.model.Book;
import com.app.bookstore.backend.model.User;
import com.app.bookstore.backend.model.WishList;
import com.app.bookstore.backend.service.WishListService;
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

@WebMvcTest(controllers = WishListController.class)
@AutoConfigureMockMvc(addFilters = false)
@ExtendWith(MockitoExtension.class)
@Import(SecurityConfig.class)
class WishListControllerTest
{
    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private UserMapper userMapper;

    @MockBean
    private WishListService wishListService;

    @MockBean
    private JWTService jwtService;

    private User user;
    private UserDetails userDetails;
    private WishListDTO wishListDTO;
    private Book book;
    private WishList wishList;

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

        book=Book.builder()
                .bookId(1L)
                .bookName("Atomic Habits")
                .price(199.90)
                .bookLogo("URL")
                .author("James Clear")
                .quantity(45)
                .description("Improve 1% everyday").build();

        wishList=WishList.builder()
                .id(1L)
                .userId(user.getUserId())
                .book(book).build();

        wishListDTO=WishListDTO.builder()
                .bookId(book.getBookId()).build();
    }

    @Test
    public void WishListController_AddToWishList_MustAddBookToWishList() throws Exception
    {
        String token="Bearer token";
        JsonResponseDTO responseDTO=new JsonResponseDTO(true,"Book added to wishlist successfully", List.of(wishList));

        given(wishListService.addToWishList(ArgumentMatchers.any(),ArgumentMatchers.any())).willReturn(responseDTO);
        given(jwtService.validateToken(ArgumentMatchers.any(),ArgumentMatchers.any())).willReturn(true);
        given(userMapper.validateUserToken(ArgumentMatchers.any())).willReturn(userDetails);

        mockMvc.perform(post("/wishList/addToWishList")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(wishListDTO))
                        .characterEncoding(StandardCharsets.UTF_8)
                        .header("Authorization",token))
                .andDo(MockMvcResultHandlers.print())
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.message", CoreMatchers.is(responseDTO.getMessage())));
    }

    @Test
    public void WishListController_RemoveFromWishList_MustRemoveBookFromWishList() throws Exception
    {
        String token="Bearer token";
        JsonResponseDTO responseDTO=new JsonResponseDTO(true,"Book removed from wishlist successfully",null);

        given(wishListService.removeFromWishList(ArgumentMatchers.any(),ArgumentMatchers.anyLong())).willReturn(responseDTO);
        given(jwtService.validateToken(ArgumentMatchers.any(),ArgumentMatchers.any())).willReturn(true);
        given(userMapper.validateUserToken(ArgumentMatchers.any())).willReturn(userDetails);

        mockMvc.perform(delete("/wishList/removeFromWishList/{bookId}",book.getBookId())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(wishListDTO))
                        .characterEncoding(StandardCharsets.UTF_8)
                        .header("Authorization",token))
                .andDo(MockMvcResultHandlers.print())
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.message", CoreMatchers.is(responseDTO.getMessage())));
    }

    @Test
    public void WishListController_GetWishList_MustReturnWishList() throws Exception
    {
        String token="Bearer token";
        JsonResponseDTO responseDTO=new JsonResponseDTO(true,"User Wishlist fetched successfully",List.of(wishList));

        given(wishListService.getAllWishListItems(ArgumentMatchers.any())).willReturn(responseDTO);
        given(jwtService.validateToken(ArgumentMatchers.any(),ArgumentMatchers.any())).willReturn(true);
        given(userMapper.validateUserToken(ArgumentMatchers.any())).willReturn(userDetails);

        mockMvc.perform(get("/wishList/getWishList")
                        .contentType(MediaType.APPLICATION_JSON)
                        .characterEncoding(StandardCharsets.UTF_8)
                        .header("Authorization",token))
                .andDo(MockMvcResultHandlers.print())
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.message", CoreMatchers.is(responseDTO.getMessage())));
    }

    @Test
    public void WishListController_IsInWishList_MustReturnTrueOrFalse() throws Exception
    {
        String token="Bearer token";
        JsonResponseDTO responseDTO=new JsonResponseDTO(true,"Book is in wishlist ",List.of(wishList));

        given(wishListService.isInWishList(ArgumentMatchers.any(),ArgumentMatchers.anyLong())).willReturn(responseDTO);
        given(jwtService.validateToken(ArgumentMatchers.any(),ArgumentMatchers.any())).willReturn(true);
        given(userMapper.validateUserToken(ArgumentMatchers.any())).willReturn(userDetails);

        mockMvc.perform(get("/wishList/isInWishList/{bookId}",book.getBookId())
                        .contentType(MediaType.APPLICATION_JSON)
                        .characterEncoding(StandardCharsets.UTF_8)
                        .header("Authorization",token))
                .andDo(MockMvcResultHandlers.print())
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.message", CoreMatchers.is(responseDTO.getMessage())));
    }
}