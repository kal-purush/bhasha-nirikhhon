package com.github.classpick.reservation.controller;

import com.github.classpick.global.dto.Response;
import com.github.classpick.reservation.service.ReservationAdminService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@Tag(name = "관리자")
@RequiredArgsConstructor
@RestController
public class ReservationAdminController {

    private final ReservationAdminService reservationAdminService;

    @Operation(summary = "강의실 예약 승인")
    @PreAuthorize("hasRole('MANAGER')")
    @PostMapping("v0.0/admin/reservation/{reservationId}/approve")
    public Response<Void> approveReservation(@PathVariable Long reservationId) {
        reservationAdminService.approveReservation(reservationId);
        return Response.ok();
    }

    @Operation(summary = "강의실 예약 거절")
    @PreAuthorize("hasRole('MANAGER')")
    @PostMapping("v0.0/admin/reservation/{reservationId}/rejected")
    public Response<Void> rejectReservation(@PathVariable Long reservationId) {
        reservationAdminService.rejectReservation(reservationId);
        return Response.ok();
    }

}