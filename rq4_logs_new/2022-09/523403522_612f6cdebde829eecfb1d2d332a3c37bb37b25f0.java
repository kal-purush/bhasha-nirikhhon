package ru.practicum.shareit.booking.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import ru.practicum.shareit.booking.dto.BookingDto;
import ru.practicum.shareit.booking.service.BookingService;
import ru.practicum.shareit.exceptions.NotFoundException;

import javax.validation.Valid;

/**
 * // TODO .
 */
@RestController
@RequestMapping(path = "/bookings")
public class BookingController {

    //private final BookingService bookingService;

    //@Autowired
    //public BookingController(BookingService bookingService) {
        //this.bookingService = bookingService;
    //}

    @PostMapping
    public BookingDto postBooking(@RequestBody @Valid BookingDto bookingDto) {
        throw new NotFoundException("Метод не готов");
    }

    @PatchMapping("/{id}")
    public BookingDto patchOwnerBooking(@PathVariable Long id,
                                        @RequestParam Boolean approved) {
        throw new NotFoundException("Метод не готов");
    }

    @GetMapping("/{id}")
    public BookingDto getBooking(@PathVariable Long id) {
        throw new NotFoundException("Метод не готов");
    }
}