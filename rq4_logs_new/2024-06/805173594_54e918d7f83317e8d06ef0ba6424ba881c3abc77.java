package com.example.BookingTravelBackend.payload.Request;

import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class RoomEditRequest {
    @NotNull(message = "vui lòng nhập Số Người")
    private int numberOfPeople;
    @NotEmpty(message = "vui lòng nhập Loại Phòng")
    private String typeRoom;
    private String describe;
    @NotNull(message = "vui lòng nhập Giá")
    private int price;
    @NotNull(message = "vui lòng nhập Tên Phòng ")
    private String roomName;
    private int roomId;
}