package ru.practicum.shareit.booking.dto;

import javax.validation.constraints.FutureOrPresent;
import javax.validation.constraints.NotNull;

import lombok.Builder;
import lombok.Data;
import ru.practicum.shareit.validation.Create;

import java.time.LocalDateTime;

@Data
@Builder
public class BookingDtoAdd {

    private Integer id;

    @NotNull(groups = {Create.class})
    @FutureOrPresent(groups = {Create.class})
    private LocalDateTime start;

    @NotNull(groups = {Create.class})
    @FutureOrPresent(groups = {Create.class})
    private LocalDateTime end;

    @NotNull
    private Integer itemId;
}