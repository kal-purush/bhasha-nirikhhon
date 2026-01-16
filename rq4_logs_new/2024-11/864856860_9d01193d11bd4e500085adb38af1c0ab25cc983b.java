package com.modsen.ratingservice.controller.impl;

import com.modsen.ratingservice.dto.request.RatingRequestDto;
import com.modsen.ratingservice.dto.response.AverageRatingResponseDto;
import com.modsen.ratingservice.dto.response.RatingResponseDto;
import com.modsen.ratingservice.service.impl.DriverRatingService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

@WebMvcTest(DriverRatingController.class)
@ExtendWith(SpringExtension.class)
class DriverRatingControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private DriverRatingService driverRatingService;

    private static final String DRIVER_RATING_ENDPOINT = "/api/v1/drivers-ratings";
    private static final String DRIVER_RATING_UPDATE_DELETE_ENDPOINT = "/api/v1/drivers-ratings/{id}";
    private static final String DRIVER_RATING_AVERAGE_ENDPOINT = "/api/v1/drivers-ratings/{refUserId}/average";

    private static final String RATING_UPDATE_REQUEST_JSON =
            "{" +
                    "\"comment\":\"Great ride!\"," +
                    " \"rating\":5" +
                    "}";
    private static final String RATING_CREATE_REQUEST_JSON =
            "{" +
                    "\"comment\":\"Great ride!\"," +
                    " \"rating\":5, " +
                    "\"rideId\":1001" +
                    "}";
    private static final String RATING_RESPONSE_JSON =
            "{" +
                    "\"id\":1," +
                    "\"comment\":\"Great ride!\"," +
                    "\"userType\":\"DRIVER\"," +
                    "\"refUserId\":1," +
                    "\"rating\":5," +
                    "\"rideId\":1001}" +
                    "";
    private static final String AVERAGE_RATING_RESPONSE_JSON =
            "{" +
                    "\"refUserId\":1," +
                    "\"averageRating\":4.5" +
                    "}";

    @Test
    @DisplayName("Test createDriverRating(RatingRequestDto)")
    void testCreateDriverRating() throws Exception {
        // Arrange
        RatingResponseDto ratingResponseDto = new RatingResponseDto(
                1L,
                "Great ride!",
                "DRIVER",
                1,
                5,
                1001L);

        when(driverRatingService.createRating(any(RatingRequestDto.class)))
                .thenReturn(ratingResponseDto);

        // Act
        mockMvc.perform(MockMvcRequestBuilders.post(DRIVER_RATING_ENDPOINT)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(RATING_CREATE_REQUEST_JSON))
                .andExpect(MockMvcResultMatchers.status().isCreated())
                .andExpect(MockMvcResultMatchers.content().contentType("application/json"))
                .andExpect(MockMvcResultMatchers.content().string(RATING_RESPONSE_JSON));
    }

    @Test
    @DisplayName("Test updateDriverRating(Long, RatingRequestDto)")
    void testUpdateDriverRating() throws Exception {
        // Arrange
        Long ratingId = 1L;
        RatingResponseDto ratingResponseDto = new RatingResponseDto(
                ratingId,
                "Great ride!",
                "DRIVER",
                1,
                5,
                1001L);

        when(driverRatingService.updateRatingById(eq(ratingId), any(RatingRequestDto.class)))
                .thenReturn(ratingResponseDto);

        // Act
        mockMvc.perform(MockMvcRequestBuilders.put(DRIVER_RATING_UPDATE_DELETE_ENDPOINT, ratingId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(RATING_UPDATE_REQUEST_JSON))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.content().contentType("application/json"))
                .andExpect(MockMvcResultMatchers.content().string(RATING_RESPONSE_JSON));
    }

    @Test
    @DisplayName("Test getDriverRating(Long)")
    void testGetDriverRating() throws Exception {
        // Arrange
        Long ratingId = 1L;
        RatingResponseDto ratingResponseDto = new RatingResponseDto(
                ratingId,
                "Great ride!",
                "DRIVER",
                1,
                5,
                1001L);

        when(driverRatingService.getRating(ratingId)).thenReturn(ratingResponseDto);

        // Act
        mockMvc.perform(MockMvcRequestBuilders.get(DRIVER_RATING_UPDATE_DELETE_ENDPOINT, ratingId))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.content().contentType("application/json"))
                .andExpect(MockMvcResultMatchers.content().string(RATING_RESPONSE_JSON));
    }

    @Test
    @DisplayName("Test deleteDriverRating(Long)")
    void testDeleteDriverRating() throws Exception {
        // Arrange
        doNothing().when(driverRatingService).safeDeleteRating(any(Long.class));

        // Act
        mockMvc.perform(MockMvcRequestBuilders.delete(DRIVER_RATING_UPDATE_DELETE_ENDPOINT, 1L))
                .andExpect(MockMvcResultMatchers.status().isNoContent());
    }

    @Test
    @DisplayName("Test averageDriverRating(Long)")
    void testAverageDriverRating() throws Exception {
        // Arrange
        Long refUserId = 1L;
        AverageRatingResponseDto averageRatingResponseDto = new AverageRatingResponseDto(refUserId, 4.5);

        when(driverRatingService.getAverageRating(refUserId)).thenReturn(averageRatingResponseDto);

        // Act
        mockMvc.perform(MockMvcRequestBuilders.get(DRIVER_RATING_AVERAGE_ENDPOINT, refUserId))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.content().contentType("application/json"))
                .andExpect(MockMvcResultMatchers.content().string(AVERAGE_RATING_RESPONSE_JSON));
    }
}