package com.reservibe.domain.usecase.reservation;

import com.reservibe.domain.entity.reservation.Reservation;
import com.reservibe.domain.input.reservation.CreateReservationInput;
import com.reservibe.infra.adapter.reservation.CreateReservationService;
import com.reservibe.infra.adapter.table.SearchTableServiceById;

public class CreateReservationUsecase {
    private final CreateReservationService createReservationService;
    private final SearchTableServiceById searchTableServiceById;
    public CreateReservationUsecase(CreateReservationService createReservationService, SearchTableServiceById searchTableServiceById) {
        this.createReservationService = createReservationService;
        this.searchTableServiceById = searchTableServiceById;
    }

    public void execute(CreateReservationInput reservation) {
        var reserv = new Reservation(reservation.client(),
                reservation.status(),reservation.reservationDate(),
                searchTableServiceById.getTable(reservation.table().getId())
                ,reservation.notesObservations());
        createReservationService.createReservation(reserv);

    }
}