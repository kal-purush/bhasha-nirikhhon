package com.modsen.ratingservice;

import com.modsen.ratingservice.client.RideResponseDto;
import com.modsen.ratingservice.dto.request.RatingRequestDto;
import com.modsen.ratingservice.dto.response.AverageRatingResponseDto;
import com.modsen.ratingservice.dto.response.RatingResponseDto;
import com.modsen.ratingservice.model.DriverRating;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public class AppTestUtil {

    //For controllers tests
    public static final String DRIVER_RATING_ENDPOINT = "/api/v1/drivers-ratings";
    public static final String DRIVER_RATING_UPDATE_DELETE_ENDPOINT = "/api/v1/drivers-ratings/{id}";
    public static final String DRIVER_RATING_AVERAGE_ENDPOINT = "/api/v1/drivers-ratings/{refUserId}/average";

    public static final RatingResponseDto ratingResponseDto = new RatingResponseDto(
            1L,
            "Great ride!",
            "DRIVER",
            1,
            5,
            1001L);

    public static final RatingRequestDto ratingUpdateRequestDto = new RatingRequestDto(
            "Great ride!",
            5,
            null);

    public static final RatingRequestDto invalidRatingUpdateRequestDto = new RatingRequestDto(
            "Great ride!",
            10,
            null);

    public static final RatingRequestDto ratingCreateRequestDto = new RatingRequestDto(
            "Great ride!",
            5,
            1001L);
    public static final RatingRequestDto invalidRatingCreateRequestDto = new RatingRequestDto(
            "",
            null,
            null);


    //For services tests
    public static final DriverRating driverRating;

    public static final RatingRequestDto ratingRequestDto;
    public static final AverageRatingResponseDto averageRatingResponseDto;
    public static final RatingResponseDto ratingResponseInServiceDto;
    public static final RideResponseDto rideResponseDto;

    static {
        driverRating = new DriverRating();
        driverRating.setComment("Excellent ride!");
        driverRating.setRating(3);
        driverRating.setRideId(1L);
        driverRating.setRefUserId(1L);
        driverRating.setDeleted(false);

        ratingRequestDto = new RatingRequestDto("Great experience!", 5, 1L);

        averageRatingResponseDto = new AverageRatingResponseDto(1L, 4.5);

        ratingResponseInServiceDto = new RatingResponseDto(
                1L,
                "Excellent ride!",
                "DRIVER",
                1,
                5,
                1L
        );
        rideResponseDto = new RideResponseDto(
                1L,
                101L,
                202L,
                "123 Main St",
                "456 Elm St",
                "COMPLETED",
                LocalDateTime.parse("2023-10-01T10:15:30"),
                BigDecimal.valueOf(15.75)
        );
    }

}