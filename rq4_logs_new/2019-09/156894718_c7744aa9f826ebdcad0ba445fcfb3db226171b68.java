package com.bin448.backend.controller;

import com.bin448.backend.entity.DTOentity.HotelReservationDTO;
import com.bin448.backend.service.HotelReservationService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/reservation")
public class HotelReservationController {

    private final HotelReservationService hotelReservationService;

    public HotelReservationController(HotelReservationService hotelReservationService) {
        this.hotelReservationService = hotelReservationService;
    }

    @PostMapping
    public ResponseEntity<String> addReservation(@RequestBody HotelReservationDTO reservation) {
        hotelReservationService.addReservation(reservation);
        return ResponseEntity.ok("Successfully added reservation");
    }

}