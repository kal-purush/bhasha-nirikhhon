package com.app.bookstore.backend.service;

import com.app.bookstore.backend.DTO.JsonResponseDTO;
import com.app.bookstore.backend.mapper.UserMapper;
import com.app.bookstore.backend.model.Address;
import com.app.bookstore.backend.model.User;
import com.app.bookstore.backend.repository.AddressRepository;
import com.app.bookstore.backend.repository.UserRepository;
import com.app.bookstore.backend.serviceimpl.AddressServiceImpl;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.test.mock.mockito.MockBean;

import java.time.LocalDate;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AddressServiceTest
{
    @Mock
    private UserRepository userRepository;

    @InjectMocks
    private AddressServiceImpl addressService;

    @Mock
    private AddressRepository addressRepository;

    @MockBean
    private UserMapper userMapper;

    private Address address;
    private User user;

    @BeforeEach
    public void init()
    {
        address=Address.builder()
                .streetName("Baner")
                .city("Pune")
                .state("Maharastra")
                .pinCode(414004)
                .addressId(1L)
                .userId(1L).build();

        List<Address> addresses=new ArrayList<>();
        addresses.add(address);

        user=User.builder()
                .userId(1L)
                .firstName("Sai")
                .lastName("Chandu")
                .email("marri@gmail.com")
                .dob(LocalDate.of(2002,8,24))
                .password("saichandu@45")
                .role("USER")
                .addresses(addresses)
                .registeredDate(LocalDate.now()).build();
    }

    @Test
    public void AddressService_AddAddress_MustAddAddress()
    {
        when(userRepository.findByEmail(Mockito.any(String.class))).thenReturn(Optional.of(user));
        when(addressRepository.save(Mockito.any(Address.class))).thenReturn(address);

        JsonResponseDTO responseDTO=addressService.addAddress(user.getEmail(),address);
        Assertions.assertThat(responseDTO).isNotNull();
        Assertions.assertThat(responseDTO.isResult()).isEqualTo(true);
    }

    @Test
    public void AddressService_EditAddress_MustEditAddress()
    {
        when(userRepository.findByEmail(Mockito.any(String.class))).thenReturn(Optional.of(user));
        when(addressRepository.findById(Mockito.any(Long.class))).thenReturn(Optional.of(address));
        when(addressRepository.save(Mockito.any(Address.class))).thenReturn(address);

        JsonResponseDTO responseDTO=addressService.editAddress(user.getEmail(),address.getAddressId(),address);
        Assertions.assertThat(responseDTO).isNotNull();
        Assertions.assertThat(responseDTO.isResult()).isEqualTo(true);
    }

    @Test
    public void AddressService_DeleteAddress_MustDeleteAddress()
    {
        when(addressRepository.findById(Mockito.any(Long.class))).thenReturn(Optional.of(address));
        when(userRepository.findByEmail(Mockito.any(String.class))).thenReturn(Optional.of(user));

        assertAll(()->addressService.deleteAddress(user.getEmail(),address.getAddressId()));
    }

    @Test
    public void AddressService_GetAllUserAddress_MustReturnAllUserAddress()
    {
        when(userRepository.findByEmail(Mockito.any(String.class))).thenReturn(Optional.of(user));

        JsonResponseDTO responseDTO=addressService.getAllUserAddress(user.getEmail());
        System.out.println(responseDTO);
        Assertions.assertThat(responseDTO).isNotNull();
        Assertions.assertThat(responseDTO.isResult()).isEqualTo(true);
    }

    @Test
    public void AddressService_GetAddressById_MustReturnAddressById()
    {
        when(userRepository.findByEmail(Mockito.any(String.class))).thenReturn(Optional.of(user));
        when(addressRepository.findById(Mockito.any(Long.class))).thenReturn(Optional.of(address));

        JsonResponseDTO responseDTO=addressService.getAddressById(user.getEmail(),address.getAddressId());
        System.out.println(responseDTO);
        Assertions.assertThat(responseDTO).isNotNull();
        Assertions.assertThat(responseDTO.isResult()).isEqualTo(true);
    }
}