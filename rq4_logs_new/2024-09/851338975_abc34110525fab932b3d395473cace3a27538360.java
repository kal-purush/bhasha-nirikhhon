package com.reservibe.application.controller.reservation.management;

import com.reservibe.application.response.GenericResponse;
import com.reservibe.application.response.PresenterResponse;
import com.reservibe.domain.output.reservation.ReservationListOutput;
import com.reservibe.domain.presenters.reservation.ReservationListPresenter;
import com.reservibe.domain.usecase.reservation.SearchReservationUseCase;
import com.reservibe.infra.adapter.reservation.SearchReservationAdapter;
import com.reservibe.infra.repository.reservation.ReservationRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/reservation/search")
public class SearchReservationController {

    private final ReservationRepository reservationRepository;

    public SearchReservationController(ReservationRepository reservationRepository) {
        this.reservationRepository = reservationRepository;
    }

    @GetMapping("/{restaurantId}")
    public ResponseEntity<Object> searchReservationById(@PathVariable UUID restaurantId) {
        SearchReservationUseCase searchReservationUseCase = new SearchReservationUseCase(new SearchReservationAdapter(reservationRepository));
        ReservationListOutput listOutput = searchReservationUseCase.execute(restaurantId);
        if (listOutput.getOutputStatus().getCode() != 200) {
            return new GenericResponse().response(listOutput);
        }
        ReservationListPresenter reservationPresenter = new ReservationListPresenter(listOutput);
        return new PresenterResponse().response(reservationPresenter);
    }
}