package com.app.bookstore.backend.controller;

import com.app.bookstore.backend.DTO.JsonResponseDTO;
import com.app.bookstore.backend.config.SecurityConfig;
import com.app.bookstore.backend.mapper.UserMapper;
import com.app.bookstore.backend.model.Address;
import com.app.bookstore.backend.model.User;
import com.app.bookstore.backend.service.AddressService;
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
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.result.MockMvcResultHandlers;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.BDDMockito.given;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;

@WebMvcTest(controllers = AddressController.class)
@AutoConfigureMockMvc(addFilters = false)
@ExtendWith(MockitoExtension.class)
@Import(SecurityConfig.class)
class AddressControllerTest
{
    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private UserMapper userMapper;

    @MockBean
    private JWTService jwtService;

    @MockBean
    private AddressService addressService;

    @MockBean
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
                .role("USER")
                .userId(1L)
                .build();
    }

    @Test
    public void AddressController_AddAddress_MustAddAddress() throws Exception
    {
        String token="Bearer Token";
        Address address=Address.builder()
                .state("Andhra pradesh")
                .city("Kadapa")
                .streetName("Gandhinagar")
                .pinCode(516004)
                .userId(user.getUserId())
                .build();

        JsonResponseDTO responseDTO=new JsonResponseDTO(true,"Address added successfully",null);

        given(addressService.addAddress(ArgumentMatchers.any(),ArgumentMatchers.any())).willReturn(responseDTO);
        given(jwtService.validateToken(ArgumentMatchers.any(),ArgumentMatchers.any())).willReturn(true);
        given(userMapper.validateUserToken(ArgumentMatchers.any())).willReturn(userDetails);

        mockMvc.perform(post("/address/addAddress")
                .contentType(MediaType.APPLICATION_JSON)
                        .header("Authorization",token)
                .characterEncoding(StandardCharsets.UTF_8)
                .content(objectMapper.writeValueAsString(address)))
                .andDo(MockMvcResultHandlers.print())
                .andExpect(MockMvcResultMatchers.status().isAccepted())
                .andExpect(MockMvcResultMatchers.jsonPath("$.message", CoreMatchers.is(responseDTO.getMessage())))
                .andExpect(MockMvcResultMatchers.jsonPath("$.result",CoreMatchers.is(responseDTO.isResult())));
    }

    @Test
    public void AddressController_EditAddress_MustEditAddress() throws Exception
    {
        String token="Bearer Token";
        Address address=Address.builder()
                .addressId(1L)
                .state("Andhra pradesh")
                .city("Kadapa")
                .streetName("Gandhinagar")
                .pinCode(516004)
                .userId(user.getUserId())
                .build();

        JsonResponseDTO responseDTO=new JsonResponseDTO(true,"Address edited successfully",null);

        given(addressService.editAddress(ArgumentMatchers.any(),ArgumentMatchers.any(),ArgumentMatchers.any())).willReturn(responseDTO);
        given(jwtService.validateToken(ArgumentMatchers.any(),ArgumentMatchers.any())).willReturn(true);
        given(userMapper.validateUserToken(ArgumentMatchers.any())).willReturn(userDetails);

        mockMvc.perform(put("/address/editAddress/{addressId}",address.getAddressId())
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("Authorization",token)
                        .characterEncoding(StandardCharsets.UTF_8)
                        .content(objectMapper.writeValueAsString(address)))
                .andDo(MockMvcResultHandlers.print())
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.message", CoreMatchers.is(responseDTO.getMessage())))
                .andExpect(MockMvcResultMatchers.jsonPath("$.result",CoreMatchers.is(responseDTO.isResult())));
    }

    @Test
    public void AddressController_DeleteAddress_MustDeleteAddress() throws Exception
    {
        String token="Bearer Token";
        Address address=Address.builder()
                .addressId(1L)
                .state("Andhra pradesh")
                .city("Kadapa")
                .streetName("Gandhinagar")
                .pinCode(516004)
                .userId(user.getUserId())
                .build();

        JsonResponseDTO responseDTO=new JsonResponseDTO(true,"Address deleted successfully",null);

        given(addressService.deleteAddress(ArgumentMatchers.any(),ArgumentMatchers.any())).willReturn(responseDTO);
        given(jwtService.validateToken(ArgumentMatchers.any(),ArgumentMatchers.any())).willReturn(true);
        given(userMapper.validateUserToken(ArgumentMatchers.any())).willReturn(userDetails);

        mockMvc.perform(delete("/address/deleteAddress/{addressId}",address.getAddressId())
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("Authorization",token)
                        .characterEncoding(StandardCharsets.UTF_8)
                        .content(objectMapper.writeValueAsString(address)))
                .andDo(MockMvcResultHandlers.print())
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.message", CoreMatchers.is(responseDTO.getMessage())))
                .andExpect(MockMvcResultMatchers.jsonPath("$.result",CoreMatchers.is(responseDTO.isResult())));
    }

    @Test
    public void AddressController_GetAllAddress_MustReturnAllAddress() throws Exception
    {
        String token="Bearer Token";
        Address address=Address.builder()
                .addressId(1L)
                .state("Andhra pradesh")
                .city("Kadapa")
                .streetName("Gandhinagar")
                .pinCode(516004)
                .userId(user.getUserId())
                .build();

        Address address2=Address.builder()
                .addressId(2L)
                .state("Andhra pradesh")
                .city("Kadapa")
                .streetName("Gandhinagar")
                .pinCode(516004)
                .userId(user.getUserId())
                .build();

        JsonResponseDTO responseDTO=new JsonResponseDTO(true,"Address deleted successfully", List.of(address,address2));

        given(addressService.getAllUserAddress(ArgumentMatchers.any())).willReturn(responseDTO);
        given(jwtService.validateToken(ArgumentMatchers.any(),ArgumentMatchers.any())).willReturn(true);
        given(userMapper.validateUserToken(ArgumentMatchers.any())).willReturn(userDetails);

        mockMvc.perform(get("/address/allAddress")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("Authorization",token)
                        .characterEncoding(StandardCharsets.UTF_8))
                .andDo(MockMvcResultHandlers.print())
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.message", CoreMatchers.is(responseDTO.getMessage())))
                .andExpect(MockMvcResultMatchers.jsonPath("$.result",CoreMatchers.is(responseDTO.isResult())))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data[0].state",CoreMatchers.is(address.getState())))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data[1].city",CoreMatchers.is(address2.getCity())));
    }

    @Test
    public void AddressController_GetAddressById_MustReturnAddress() throws Exception
    {
        String token="Bearer Token";
        Address address=Address.builder()
                .addressId(1L)
                .state("Andhra pradesh")
                .city("Kadapa")
                .streetName("Gandhinagar")
                .pinCode(516004)
                .userId(user.getUserId())
                .build();

        JsonResponseDTO responseDTO=new JsonResponseDTO(true,"Address deleted successfully", List.of(address));

        given(addressService.getAddressById(ArgumentMatchers.any(),ArgumentMatchers.any())).willReturn(responseDTO);
        given(jwtService.validateToken(ArgumentMatchers.any(),ArgumentMatchers.any())).willReturn(true);
        given(userMapper.validateUserToken(ArgumentMatchers.any())).willReturn(userDetails);

        mockMvc.perform(get("/address/getAddressById/{addressId}",address.getAddressId())
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("Authorization",token)
                        .characterEncoding(StandardCharsets.UTF_8))
                .andDo(MockMvcResultHandlers.print())
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.message", CoreMatchers.is(responseDTO.getMessage())))
                .andExpect(MockMvcResultMatchers.jsonPath("$.result",CoreMatchers.is(responseDTO.isResult())))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data[0].state",CoreMatchers.is(address.getState())));
    }
}