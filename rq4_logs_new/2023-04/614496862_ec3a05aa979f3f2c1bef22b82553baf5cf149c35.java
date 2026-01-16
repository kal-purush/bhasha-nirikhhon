package ru.practicum.shareit.booking.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import ru.practicum.shareit.booking.dto.BookingFromFrontDto;
import ru.practicum.shareit.booking.dto.BookingToFrontDto;
import ru.practicum.shareit.booking.model.BookingSearchStatus;
import ru.practicum.shareit.booking.service.BookingService;
import ru.practicum.shareit.helper.Helpers;

import java.util.List;

@RestController
@RequestMapping(path = "/bookings")
@RequiredArgsConstructor
public class BookingController {
    private final BookingService bookingService;
    private static final String HEADER = "X-Sharer-User-Id";

    @GetMapping
    public List<BookingToFrontDto> getAll(@RequestHeader(HEADER) Long userId,
                                          @RequestParam(defaultValue = "ALL") String state) {
        BookingSearchStatus serchStatus = BookingSearchStatus.getStatus(state);
        return bookingService.getAllBookings(userId, serchStatus);
    }

    @PostMapping
    public BookingToFrontDto add(@Validated @RequestBody BookingFromFrontDto bookingFromFrontDto,
                                 @RequestHeader(HEADER) Long userId) {
        Helpers.checkDatesValidity(bookingFromFrontDto.getStart(), bookingFromFrontDto.getEnd());
        return bookingService.saveBooking(userId, bookingFromFrontDto);
    }

    @GetMapping("/{bookingId}")
    public BookingToFrontDto findBooking(@PathVariable Long bookingId,
                                         @RequestHeader(HEADER) Long userId) {
        return bookingService.findBooking(bookingId, userId);
    }

    @PatchMapping("/{bookingId}")
    public BookingToFrontDto approveBooking(@PathVariable Long bookingId,
                                            @RequestHeader(HEADER) Long userId,
                                            @RequestParam Boolean approved) {
        return bookingService.findBookingForApprove(bookingId, userId, approved);
    }

    @GetMapping("/owner")
    public List<BookingToFrontDto> searchItems(@RequestHeader(HEADER) Long userId,
                                               @RequestParam(defaultValue = "ALL") String state) {
        BookingSearchStatus serchStatus = BookingSearchStatus.getStatus(state);
        return bookingService.findBookingsByOwner(userId, serchStatus);
    }

    @DeleteMapping("/{bookingId}")
    public void deleteBooking(@PathVariable Long bookingId,
                              @RequestHeader(HEADER) Long userId) {
        bookingService.deleteBooking(bookingId, userId);
    }
}