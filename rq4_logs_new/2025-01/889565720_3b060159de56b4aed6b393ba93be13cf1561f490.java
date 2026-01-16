package com.app.bookstore.backend.controller;

import com.app.bookstore.backend.DTO.JsonResponseDTO;
import com.app.bookstore.backend.DTO.OrderDTO;
import com.app.bookstore.backend.config.SecurityConfig;
import com.app.bookstore.backend.mapper.UserMapper;
import com.app.bookstore.backend.model.Order;
import com.app.bookstore.backend.model.User;
import com.app.bookstore.backend.service.OrderService;
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

@WebMvcTest(controllers = OrderController.class)
@AutoConfigureMockMvc(addFilters = false)
@ExtendWith(MockitoExtension.class)
@Import(SecurityConfig.class)
class OrderControllerTest
{
    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private JWTService jwtService;

    @MockBean
    private UserMapper userMapper;

    @MockBean
    private OrderService orderService;

    private UserDetails userDetails;
    private User user;
    private User admin;
    private UserDetails adminDetails;
    private OrderDTO orderDTO;
    private Order order;

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

        orderDTO=OrderDTO.builder()
                .quantity(1)
                .price(2999.90)
                .addressId(1L).build();

        order=Order.builder()
                .orderId(1L)
                .orderDate(LocalDate.now())
                .cancelOrder(false)
                .orderQuantity(orderDTO.getQuantity())
                .orderPrice(orderDTO.getPrice())
                .addressId(orderDTO.getAddressId())
                .carts(null)
                .userId(user.getUserId()).build();
    }

    @Test
    public void OrderController_PlaceOrder_MustPlaceOrder() throws Exception
    {
        String token="Bearer token";
        JsonResponseDTO responseDTO=new JsonResponseDTO(true,"Order placed successfully", List.of(order));

        given(orderService.placeOrder(ArgumentMatchers.any(),ArgumentMatchers.any())).willReturn(responseDTO);
        given(jwtService.validateToken(ArgumentMatchers.any(),ArgumentMatchers.any())).willReturn(true);
        given(userMapper.validateUserToken(ArgumentMatchers.any())).willReturn(userDetails);

        mockMvc.perform(post("/order/placeOrder")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(orderDTO))
                        .characterEncoding(StandardCharsets.UTF_8)
                        .header("Authorization",token))
                .andDo(MockMvcResultHandlers.print())
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.message", CoreMatchers.is(responseDTO.getMessage())));
    }

    @Test
    public void OrderController_CancelOrder_MustCancelOrder() throws Exception
    {
        String token="Bearer token";
        Order orderCancel=Order.builder()
                .orderId(1L)
                .orderDate(order.getOrderDate())
                .cancelOrder(true)
                .orderQuantity(orderDTO.getQuantity())
                .orderPrice(orderDTO.getPrice())
                .addressId(orderDTO.getAddressId())
                .carts(null)
                .userId(user.getUserId()).build();

        JsonResponseDTO responseDTO=new JsonResponseDTO(true,"Order cancelled successfully", List.of(orderCancel));

        given(orderService.cancelOrder(ArgumentMatchers.any(),ArgumentMatchers.any())).willReturn(responseDTO);
        given(jwtService.validateToken(ArgumentMatchers.any(),ArgumentMatchers.any())).willReturn(true);
        given(userMapper.validateUserToken(ArgumentMatchers.any())).willReturn(userDetails);

        mockMvc.perform(delete("/order/cancelOrder/{id}",order.getOrderId())
                        .contentType(MediaType.APPLICATION_JSON)
                        .characterEncoding(StandardCharsets.UTF_8)
                        .header("Authorization",token))
                .andDo(MockMvcResultHandlers.print())
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.message", CoreMatchers.is(responseDTO.getMessage())));
    }

    @Test
    public void OrderController_GetALlUserOrders_MustReturnAllUserOrders() throws Exception
    {
        String token="Bearer token";
        JsonResponseDTO responseDTO=new JsonResponseDTO(true,"User Orders fetched successfully", List.of(order));

        given(orderService.getAllOrdersForUser(ArgumentMatchers.any())).willReturn(responseDTO);
        given(jwtService.validateToken(ArgumentMatchers.any(),ArgumentMatchers.any())).willReturn(true);
        given(userMapper.validateUserToken(ArgumentMatchers.any())).willReturn(userDetails);

        mockMvc.perform(get("/order/getAllUserOrders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .characterEncoding(StandardCharsets.UTF_8)
                        .header("Authorization",token))
                .andDo(MockMvcResultHandlers.print())
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.message", CoreMatchers.is(responseDTO.getMessage())));
    }

    @Test
    public void OrderController_GetALlOrders_MustReturnAllOrders() throws Exception
    {
        String token="Bearer token";
        JsonResponseDTO responseDTO=new JsonResponseDTO(true,"All Orders fetched successfully for ADMIN", List.of(order));

        given(orderService.getAllOrders()).willReturn(responseDTO);
        given(jwtService.validateToken(ArgumentMatchers.any(),ArgumentMatchers.any())).willReturn(true);
        given(userMapper.validateUserToken(ArgumentMatchers.any())).willReturn(adminDetails);

        mockMvc.perform(get("/order/getAllOrders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .characterEncoding(StandardCharsets.UTF_8)
                        .header("Authorization",token))
                .andDo(MockMvcResultHandlers.print())
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.message", CoreMatchers.is(responseDTO.getMessage())));
    }
}