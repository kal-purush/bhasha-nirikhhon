package ru.practicum.shareit.booking.dto;

import jakarta.validation.constraints.Future;
import jakarta.validation.constraints.FutureOrPresent;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

import java.time.LocalDateTime;

@Data
public class BookingRequestDto {

    @NotNull(message = "Идентификатор бронируемой вещи не должен быть пустым")
    private Long itemId;

    @NotNull(message = "Дата начала бронирования не должна быть пустой")
    @FutureOrPresent(message = "Дата начала бронирования должны быть текущей или будущей")
    private LocalDateTime start;

    @NotNull(message = "Дата окончания бронирования не должна быть пустой")
    @Future(message = "Дата окончания бронирования должна быть в будущем")
    private LocalDateTime end;
}