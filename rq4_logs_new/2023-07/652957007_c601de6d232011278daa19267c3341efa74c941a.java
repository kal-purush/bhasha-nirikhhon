package ru.practicum.ewmmainservice.request.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import ru.practicum.ewmmainservice.event.model.EventFullDto;

import java.time.LocalDateTime;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class RequestDtoWithoutUser {

    private Integer id;

    private LocalDateTime created;

    private EventFullDto event;

    private Integer requester;

    private RequestStatus status;

}