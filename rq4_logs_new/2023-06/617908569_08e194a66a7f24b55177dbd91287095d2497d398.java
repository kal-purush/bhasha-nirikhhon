package ru.yandex.practicum.filmorate.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Event {
    @NotBlank
    private String name;
    @NotNull
    private long eventId;
    private Long timestamp;            //Храню числом из-за тестов
    @NotNull
    int userId;
    @NotNull
    int entityId;
    @NotBlank
    private String eventType;
}