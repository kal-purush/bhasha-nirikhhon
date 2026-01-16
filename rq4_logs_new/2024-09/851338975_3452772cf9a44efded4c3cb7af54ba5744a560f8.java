package com.reservibe.domain.usecase.reservation;

import com.reservibe.domain.entity.reservation.Reservation;
import com.reservibe.domain.input.reservation.ReservationManagementInput;
import com.reservibe.infra.adapter.reservation.ManagementReservationAdapter;

import java.util.UUID;

public class ManagementReservationUsecase {
    private final ManagementReservationAdapter managementReservationAdapter;

    public ManagementReservationUsecase(ManagementReservationAdapter managementReservationAdapter) {
        this.managementReservationAdapter = managementReservationAdapter;
    }

    public void execute(UUID id, ReservationManagementInput reservation) {
        var reserv = managementReservationAdapter.findById(id);

        Reservation reservationUpdate = new Reservation(reserv.getId(),
                reservation.client(),
                reservation.status(),
                reservation.reservationDate(),
                reserv.getTable(),
                reservation.notesObservations());
        managementReservationAdapter.updateReserver(reservationUpdate);

    }
}