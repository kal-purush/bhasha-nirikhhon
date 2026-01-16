package com.github.classpick.reservation.controller.dto.response;

import com.github.classpick.reservation.repository.ReservationEntity;
import com.github.classpick.reservation.repository.Status;
import com.github.classpick.room.controller.dto.response.RoomResponse;
import com.github.classpick.user.controller.dto.response.UserResponse;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.time.LocalDate;
import java.time.LocalTime;

@Getter
@AllArgsConstructor(staticName = "of")
public class UserReservationResponse {

    UserResponse user;

    RoomResponse room;

    long reservationId;

    LocalDate date;

    LocalTime startTime;

    LocalTime endTime;

    int people;

    String purpose;

    Status status;

    boolean ocrVerified;


    public static UserReservationResponse from(ReservationEntity reservation) {

        return from(reservation, false);
    }

    public static UserReservationResponse from(ReservationEntity reservation, boolean ocrVerified) {

        return of(
                UserResponse.from(reservation.getUser()),
                RoomResponse.from(reservation.getRoom()),
                reservation.getReservationId(),
                reservation.getDate(),
                reservation.getStartTime(),
                reservation.getEndTime(),
                reservation.getPeople(),
                reservation.getPurpose(),
                reservation.getStatus(),
                ocrVerified
        );
    }

}
