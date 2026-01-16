package ru.practicum.shareit.booking.dto;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.constraints.Future;
import javax.validation.constraints.FutureOrPresent;
import javax.validation.constraints.NotNull;
import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
public class NewBookingDto {

    @NotNull(message = "itemId can not be null")
    private Long itemId;

    @NotNull(message = "start_date can not be null")
    @FutureOrPresent(message = "start_date should be in the future or present")
    private LocalDateTime start;

    @NotNull(message = "end_date can not be null")
    @Future(message = "end_date should be in the future")
    private LocalDateTime end;
}