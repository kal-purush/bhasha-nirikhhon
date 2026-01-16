package com.github.classpick.reservation.controller.dto.response;

import com.github.classpick.reservation.repository.NoshowEntity;
import com.github.classpick.room.controller.dto.response.RoomResponse;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor(staticName = "of")
public class NoshowResponse {

    RoomResponse room;

    long noshowId;

    boolean verified;

    public static NoshowResponse from(NoshowEntity noshow) {

        return of(
            RoomResponse.from(noshow.getReservation().getRoom()),
            noshow.getNoshowId(),
            noshow.isVerified()
        );
    }
}