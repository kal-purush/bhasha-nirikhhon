package com.modsen.ratingservice;

import com.modsen.ratingservice.client.RideResponseDto;
import com.modsen.ratingservice.dto.request.RatingRequestDto;
import com.modsen.ratingservice.dto.response.AverageRatingResponseDto;
import com.modsen.ratingservice.dto.response.RatingResponseDto;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public class AppIntegrationTestUtil {

    public static final long REF_USER_ID = 1L;
    public static final long RATING_ID = 1L;
    public static final long RIDE_ID = 1L;
    public static final long SECOND_RIDE_ID = 2L;

    private static final long ID_AFTER_CREATE = 2L;

    private static final int DEFAULT_RATING = 5;
    private static final int UPDATED_RATING = 2;
    private static final double AVERAGE_RATING = 5.0;

    private static final String USER_TYPE = "DRIVER";
    private static final String DEFAULT_COMMENT = "Great driver, very polite!";
    private static final String UPDATED_COMMENT = "Awful driver";

    private static final String RIDE_DEPARTURE_ADDRESS = "Vilnius";
    private static final String RIDE_DESTINATION_ADDRESS = "Riga";
    private static final String RIDE_STATUS = "CREATED";
    private static final LocalDateTime RIDE_CREATION_TIME = LocalDateTime.parse("2023-10-31T14:30:00");
    private static final BigDecimal RIDE_COST = new BigDecimal(231);

    public static final String SQL_DELETE_ALL_DATA = """
            DELETE FROM driver_rating;
            """;

    public static final String SQL_RESTART_SEQUENCES = """
            ALTER SEQUENCE driver_rating_seq RESTART WITH 1;
            """;

    public static final String SQL_INSERT_DATA = """
            INSERT INTO driver_rating (id, comment, rating, ride_id, ref_user_id, deleted)
            VALUES (nextval('driver_rating_seq'), 'Great driver, very polite!', 5, 1, 1, false);
            """;

    public static final String GET_RIDE_FROM_ANOTHER_SERVICE_ENDPOINT = "/api/v1/rides/" + SECOND_RIDE_ID;

    public static final RideResponseDto RIDE_RESPONSE_DTO = new RideResponseDto(
            ID_AFTER_CREATE,
            REF_USER_ID,
            REF_USER_ID,
            RIDE_DEPARTURE_ADDRESS,
            RIDE_DESTINATION_ADDRESS,
            RIDE_STATUS,
            RIDE_CREATION_TIME,
            RIDE_COST);

    public static final RatingRequestDto RATING_REQUEST_CREATE_DTO = new RatingRequestDto(
            DEFAULT_COMMENT,
            DEFAULT_RATING,
            SECOND_RIDE_ID);

    public static final RatingResponseDto RATING_RESPONSE_CREATE_DTO = new RatingResponseDto(
            ID_AFTER_CREATE,
            DEFAULT_COMMENT,
            USER_TYPE,
            REF_USER_ID,
            DEFAULT_RATING,
            SECOND_RIDE_ID);

    public static final RatingRequestDto RATING_REQUEST_UPDATE_DTO = new RatingRequestDto(
            UPDATED_COMMENT,
            UPDATED_RATING,
            null);

    public static final RatingResponseDto RATING_RESPONSE_UPDATE_DTO = new RatingResponseDto(
            RATING_ID,
            UPDATED_COMMENT,
            USER_TYPE,
            REF_USER_ID,
            UPDATED_RATING,
            RIDE_ID);

    public static RatingResponseDto RATING_RESPONSE_GET_DTO = new RatingResponseDto(
            RATING_ID,
            DEFAULT_COMMENT,
            USER_TYPE,
            REF_USER_ID,
            DEFAULT_RATING,
            RIDE_ID);

    public static final AverageRatingResponseDto AVERAGE_RATING_RESPONSE_DTO = new AverageRatingResponseDto(
            REF_USER_ID,
            AVERAGE_RATING);

}