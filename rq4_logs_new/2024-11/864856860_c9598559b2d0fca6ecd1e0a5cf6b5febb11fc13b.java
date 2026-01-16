package com.modsen.ridesservice.controller.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.modsen.ridesservice.client.driver.DriverResponseDto;
import com.modsen.ridesservice.dto.ListContainerResponseDto;
import com.modsen.ridesservice.dto.request.RideRequestDto;
import com.modsen.ridesservice.dto.request.RideStatusRequestDto;
import com.modsen.ridesservice.dto.response.RideResponseDto;
import com.modsen.ridesservice.service.RideService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(RideController.class)
@ExtendWith(SpringExtension.class)
class RideControllerTest {

    @MockBean
    private RideService rideService;

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    private static final String RIDE_PAGE_RESPONSE_JSON =
            "{" +
                    "\"currentOffset\":1," +
                    "\"currentLimit\":1," +
                    "\"totalPages\":1," +
                    "\"totalElements\":1," +
                    "\"sort\":\"Sort\"," +
                    "\"values\":[]" +
                    "}";

    @Test
    @DisplayName("Test getPageRides(Integer, Integer)")
    void testGetPageRides() throws Exception {
        //Arrange
        when(rideService.getPageRides(any(Integer.class), any(Integer.class)))
                .thenReturn(new ListContainerResponseDto<>(
                        1, 1, 1, 1L,
                        "Sort", new ArrayList<>()
                ));
        //Act and Assert
        mockMvc.perform(get("/api/v1/rides"))
                .andExpect(status().isOk())
                .andExpect(content().contentType("application/json"))
                .andExpect(content().string(RIDE_PAGE_RESPONSE_JSON));
    }

    @Test
    @DisplayName("Test getRideById(Long)")
    void testGetRideById() throws Exception {
        //Arrange
        RideResponseDto rideResponseDto = new RideResponseDto(
                1L,
                1L,
                1L,
                "Vilnius",
                "Riga",
                "CREATED",
                LocalDateTime.parse("2023-10-31T14:30:00"),
                new BigDecimal(231));
        when(rideService.getRideById(any(Long.class)))
                .thenReturn(rideResponseDto);

        //Act and Assert
        mockMvc.perform(get("/api/v1/rides/1"))
                .andExpect(status().isOk())
                .andExpect(content().contentType("application/json"))
                .andExpect(content().string(objectMapper.writeValueAsString(rideResponseDto)));
    }

    @Test
    @DisplayName("Test getPageRideByDriverId(Long)")
    void testGetPageRideByDriverId() throws Exception {
        //Arrange
        when(rideService.getPageRidesByDriverId(any(Long.class), any(Integer.class), any(Integer.class)))
                .thenReturn(new ListContainerResponseDto<>(
                        1, 1, 1, 1L,
                        "Sort", new ArrayList<>()
                ));

        //Act and Assert
        mockMvc.perform(get("/api/v1/rides/drivers/{driverId}", 1L))
                .andExpect(status().isOk())
                .andExpect(content().contentType("application/json"))
                .andExpect(content().string(RIDE_PAGE_RESPONSE_JSON));
    }

    @Test
    @DisplayName("Test getPageRideByPassengerId(Long)")
    void testGetPageRideByPassengerId() throws Exception {
        //Arrange
        when(rideService.getPageRidesByPassengerId(any(Long.class), any(Integer.class), any(Integer.class)))
                .thenReturn(new ListContainerResponseDto<>(
                        1, 1, 1, 1L,
                        "Sort", new ArrayList<>()
                ));

        //Act and Assert
        mockMvc.perform(get("/api/v1/rides/passengers/{passengerId}", 1L))
                .andExpect(status().isOk())
                .andExpect(content().contentType("application/json"))
                .andExpect(content().string(RIDE_PAGE_RESPONSE_JSON));
    }

    @Test
    @DisplayName("Test createRide(RideRequestDto)")
    void testCreateRide() throws Exception {
        //Arrange
        RideResponseDto rideResponseDto = new RideResponseDto(
                1L,
                1L,
                1L,
                "Vilnius",
                "Riga",
                "CREATED",
                LocalDateTime.parse("2023-10-31T14:30:00"),
                new BigDecimal(231));
        RideRequestDto rideRequestDto = new RideRequestDto(
               1L,
               1L,
               "Vilnius",
               "Riga");
        MockHttpServletRequestBuilder requestBuilder = MockMvcRequestBuilders
                .post("/api/v1/rides")
                .contentType("application/json")
                .content(objectMapper.writeValueAsString(rideRequestDto));
        when(rideService.createRide(any(RideRequestDto.class))).thenReturn(rideResponseDto);

        //Act and Assert
        mockMvc.perform(requestBuilder)
                .andExpect(status().isCreated())
                .andExpect(content().contentType("application/json"))
                .andExpect(content().string(objectMapper.writeValueAsString(rideResponseDto)));
    }

    @Test
    @DisplayName("Test changeRideStatus(Long, RideStatusRequestDto)")
    void testChangeRideStatus() throws Exception {
        //Arrange
        RideStatusRequestDto rideStatusRequestDto = new RideStatusRequestDto("ACCEPTED");
        RideResponseDto rideResponseDto = new RideResponseDto(
                1L,
                1L,
                1L,
                "Vilnius",
                "Riga",
                "ACCEPTED",
                LocalDateTime.parse("2023-10-31T14:30:00"),
                new BigDecimal(231));
        MockHttpServletRequestBuilder requestBuilder = MockMvcRequestBuilders
                .patch("/api/v1/rides/{rideId}/status", 1L)
                .contentType("application/json")
                .content(objectMapper.writeValueAsString(rideStatusRequestDto));
        when(rideService.changeRideStatus(1L, rideStatusRequestDto))
                .thenReturn(rideResponseDto);

        //Act and Assert
        mockMvc.perform(requestBuilder)
                .andExpect(status().isOk())
                .andExpect(content().contentType("application/json"))
                .andExpect(content().string(objectMapper.writeValueAsString(rideResponseDto)));
    }

    @Test
    @DisplayName("Test updateRide(Long. RideRequestDto")
    void testUpdateRide() throws Exception {
        //Arrange
        RideRequestDto rideRequestDto = new RideRequestDto(
                1L,
                1L,
                "Vilnius",
                "Riga");
        RideResponseDto rideResponseDto = new RideResponseDto(
                1L,
                1L,
                1L,
                "Vilnius",
                "Riga",
                "CREATED",
                LocalDateTime.parse("2023-10-31T14:30:00"),
                new BigDecimal(231));
        MockHttpServletRequestBuilder requestBuilder = MockMvcRequestBuilders
                .put("/api/v1/rides/{rideId}", 1L)
                .contentType("application/json")
                .content(objectMapper.writeValueAsString(rideRequestDto));
        when(rideService.updateRide(1L, rideRequestDto)).thenReturn(rideResponseDto);

        //Act and Assert
        mockMvc.perform(requestBuilder)
                .andExpect(status().isOk())
                .andExpect(content().contentType("application/json"))
                .andExpect(content().string(objectMapper.writeValueAsString(rideResponseDto)));
    }

}