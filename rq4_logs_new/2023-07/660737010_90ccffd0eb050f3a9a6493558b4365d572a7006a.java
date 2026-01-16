package com.example.oneulnail.domain.reservation.controller;


import com.example.oneulnail.domain.reservation.dto.request.ReservationRegisterReqDto;
import com.example.oneulnail.domain.reservation.dto.response.ReservationRegisterResDto;
import com.example.oneulnail.domain.reservation.service.ReservationService;
import com.example.oneulnail.global.entity.BaseResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/reservation")
@RequiredArgsConstructor
public class ReservationController {

    private final ReservationService reservationService;

    @PostMapping
    public BaseResponse<ReservationRegisterResDto> register(@RequestBody ReservationRegisterReqDto reservationRegisterReqDto) {
        ReservationRegisterResDto registerResDto = reservationService.reservate(reservationRegisterReqDto);
        return BaseResponse.onSuccess(registerResDto);
    }

}