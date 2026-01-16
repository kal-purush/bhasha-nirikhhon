package ru.practicum.shareit.booking.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.practicum.shareit.booking.dto.RequestBookingDto;
import ru.practicum.shareit.booking.dto.State;
import ru.practicum.shareit.booking.service.BookingClient;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("bookings")
public class BookingController {
    private final BookingClient bookingClient;
    public static final String USER_ID = "X-Sharer-User-Id";

    @PostMapping
    public ResponseEntity<Object> create(@Valid @RequestBody RequestBookingDto booking,
                                         @RequestHeader(USER_ID) long userId) {
        log.info("Получен POST запрос на создание бронирования {} пользователем с id = {}", booking, userId);
        return bookingClient.create(userId, booking);
    }

    @PatchMapping("/{bookingId}")
    public ResponseEntity<Object> update(@PathVariable long bookingId,
                                         @RequestHeader(USER_ID) long ownerId,
                                         @RequestParam boolean approved) {
        log.info("Получен PATCH запрос на подтверждение или отклонение запроса с bookingId = {} на бронирование " +
                "от владельца с ownerId = {}, статус подтверждения: {}", bookingId, ownerId, approved);
        return bookingClient.update(ownerId, bookingId, approved);
    }

    @GetMapping("/{bookingId}")
    public ResponseEntity<Object> findById(@PathVariable long bookingId,
                                           @RequestHeader(USER_ID) long userId) {
        log.info("Получен GET запрос на получение бронирования с bookingId = {}, от пользователя с id = {}",
                bookingId, userId);
        return bookingClient.findById(userId, bookingId);
    }

    @GetMapping
    public ResponseEntity<Object> findByBooker(@RequestHeader(USER_ID) long bookerId,
                                               @RequestParam(defaultValue = "ALL") State state) {
        log.info("Получен GET запрос на получение всех бронирований текущего пользователя с bookerId = {}, state = {}",
                bookerId, state);
        return bookingClient.findByBooker(bookerId, state);
    }

    @GetMapping("/owner")
    public ResponseEntity<Object> findByOwner(@RequestHeader(USER_ID) long ownerId,
                                              @RequestParam(defaultValue = "ALL") State state) {
        log.info("Получен GET запрос на получение списка бронирований для всех вещей текущего пользователя " +
                        "с ownerId = {}, state = {}", ownerId, state);
        return bookingClient.findByOwner(ownerId, state);
    }
}