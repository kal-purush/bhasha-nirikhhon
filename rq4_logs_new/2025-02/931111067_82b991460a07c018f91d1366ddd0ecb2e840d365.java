package ru.practicum.main_service.event.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import ru.practicum.main_service.event.dto.AdminEventParams;
import ru.practicum.main_service.event.dto.EventFullDto;
import ru.practicum.main_service.event.dto.UpdateEventAdminRequest;
import ru.practicum.main_service.event.service.EventService;

import java.time.LocalDateTime;
import java.util.List;

@RestController
@RequestMapping("/admin/events")
@RequiredArgsConstructor
@Validated
@Slf4j
public class EventAdminController {
    private final EventService eventService;

    @GetMapping
    public List<EventFullDto> adminGetEvents(@RequestParam(required = false) List<Integer> users,
                                             @RequestParam(required = false) List<String> states,
                                             @RequestParam(required = false) List<Integer> categories,
                                             @RequestParam(required = false) @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime rangeStart,
                                             @RequestParam(required = false) @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime rangeEnd,
                                             @RequestParam(required = false, defaultValue = "0") Integer from,
                                             @RequestParam(required = false, defaultValue = "10") Integer size) {
        AdminEventParams adminEventParams = new AdminEventParams(users, states, categories, rangeStart, rangeEnd, from, size);
        log.info("Getting events with params {} - Started", adminEventParams);
        List<EventFullDto> events = eventService.adminGetAllEvents(adminEventParams);
        log.info("Getting events with params {} - Finished", adminEventParams);
        return events;
    }

    @PatchMapping("/{eventId}")
    public EventFullDto adminUpdateEvent(@PathVariable("eventId") Integer eventId,
                                   @Valid @RequestBody UpdateEventAdminRequest updateEventAdminRequest) {
        log.info("Updating event {} - Started", eventId);
        EventFullDto event = eventService.adminUpdateEvent(eventId, updateEventAdminRequest);
        log.info("Updating event {} - Finished", eventId);
        return event;
    }
}