package dat3.adventureXP.api;

import dat3.adventureXP.dto.ReservationResponse;
import dat3.adventureXP.service.ReservationService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/adventure/reservations")
public class ReservationController {

    ReservationService reservationService;

    public ReservationController(ReservationService reservationService) {
        this.reservationService = reservationService;
    }

    //Employee
    @GetMapping
    public List<ReservationResponse>  getReservations(){
        List<ReservationResponse> res = reservationService.getReservations();
        return res;
    }

}