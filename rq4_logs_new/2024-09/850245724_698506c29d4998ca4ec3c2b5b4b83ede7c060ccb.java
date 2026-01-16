package ru.practicum.event.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import ru.practicum.event.dto.EventFullDto;
import ru.practicum.event.dto.EventShortDto;
import ru.practicum.event.dto.NewEventDto;
import ru.practicum.event.dto.UpdateEventUserRequest;
import ru.practicum.event.service.EventService;
import ru.practicum.requests.dto.EventRequestStatusUpdateRequestDto;
import ru.practicum.requests.dto.ParticipationRequestDto;

import java.util.List;

@RestController
@RequestMapping("users/{userId}/events")
@RequiredArgsConstructor
@Slf4j
@Validated
public class EventPrivateController {
    private final EventService eventService;

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public EventFullDto addEvent(@RequestBody @Valid NewEventDto newEventDto, @PathVariable long userId) {
        log.info("Received a POST request to add event {} from a user with an userId = {}", newEventDto, userId);
        return eventService.addEvent(newEventDto, userId);
    }

    @GetMapping("/{eventId}")
    public EventFullDto findEventById(@PathVariable long userId, @PathVariable long eventId) {
        log.info("Received a GET request to find event by id {} from a user with an userId = {}", eventId, userId);
        return eventService.findEventById(userId, eventId);
    }

    @GetMapping
    public List<EventShortDto> findEventsByUser(@PathVariable long userId,
                                                @RequestParam(defaultValue = "0") long from,
                                                @RequestParam(defaultValue = "10") long size) {
        log.info("Received a GET request to find events by userId = {} from = {} size = {}", userId, from, size);
        return eventService.findEventsByUser(userId, from, size);
    }

    @PatchMapping("/{eventId}")
    public EventFullDto updateEvent(@RequestBody @Valid UpdateEventUserRequest updateEventUserRequest,
                                    @PathVariable long userId,
                                    @PathVariable long eventId) {
        log.info("Received a PATCH request to update event with an eventId = {} from a user with an userId = {}, " +
                "request body {}", eventId, userId, updateEventUserRequest);
        return eventService.updateEvent(updateEventUserRequest, userId, eventId);
    }

    @GetMapping("/{eventId}/requests")
    public List<ParticipationRequestDto> findRequestByEventId(@PathVariable long userId, @PathVariable long eventId) {
        log.info("Received a GET request to find request by event id = {} from a user with an userId = {}",
                eventId, userId);
        return eventService.findRequestByEventId(userId, eventId);
    }

    @PatchMapping("/{eventId}/requests")
    public List<ParticipationRequestDto> updateRequestByEventId(@RequestBody @Valid EventRequestStatusUpdateRequestDto
                                                                            updateRequests,
                                                                @PathVariable long userId,
                                                                @PathVariable long eventId) {
        log.info("Received a PATCH request to update request with an eventId = {} from a user with an userId = {}, " +
                "request body {}", eventId, userId, updateRequests);
        return eventService.updateRequestByEventId(updateRequests, userId, eventId);
    }
}