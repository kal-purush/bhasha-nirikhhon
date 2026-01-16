package ru.practicum.shareit.booking.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;
import ru.practicum.shareit.booking.util.MarkerForCreate;
import ru.practicum.shareit.booking.util.MarkerForUpdate;
import ru.practicum.shareit.booking.util.StartEndTime;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Null;
import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@StartEndTime(groups = MarkerForCreate.class)
public class BookingDtoForCreateUpdate {
    @Null(groups = MarkerForCreate.class)
    @NotNull(groups = MarkerForUpdate.class)
    private Integer id;
    @NotNull(groups = MarkerForCreate.class)
    private int itemId;
    private LocalDateTime start;
    private LocalDateTime end;
}