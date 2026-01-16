package com.project.ipyang.domain.point.controller;

import com.project.ipyang.common.response.ResponseDto;
import com.project.ipyang.config.SessionUser;
import com.project.ipyang.domain.point.dto.PointChargeDto;
import com.project.ipyang.domain.point.service.PointService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpSession;

@RestController
@RequiredArgsConstructor
public class PointController {
    private final PointService pointService;
    private final HttpSession session;

    @PostMapping(value = "/v1/point/charge")
    public ResponseDto chargePoint(@RequestBody PointChargeDto request){

        SessionUser loggedInUser = (SessionUser) session.getAttribute("loggedInUser");
        if (loggedInUser == null) {
            return new ResponseDto("로그인이 필요합니다", HttpStatus.INTERNAL_SERVER_ERROR.value());
        }
        Long memberId = loggedInUser.getId();
        if (memberId == null) {
            return new ResponseDto("존재하지 않는 회원입니다", HttpStatus.INTERNAL_SERVER_ERROR.value());
        }
        return  pointService.chargePoint(request,memberId);
    }

    @PostMapping(value = "/v1/point/expense")
    public ResponseDto expensePoint(@RequestBody PointChargeDto request){

        SessionUser loggedInUser = (SessionUser) session.getAttribute("loggedInUser");
        if (loggedInUser == null) {
            return new ResponseDto("로그인이 필요합니다", HttpStatus.INTERNAL_SERVER_ERROR.value());
        }
        Long memberId = loggedInUser.getId();
        if (memberId == null) {
            return new ResponseDto("존재하지 않는 회원입니다", HttpStatus.INTERNAL_SERVER_ERROR.value());
        }
        return  pointService.expensePoint(request,memberId);
    }

    @PostMapping(value = "/v1/point/income")
    public ResponseDto incomePoint(@RequestBody PointChargeDto request){

        SessionUser loggedInUser = (SessionUser) session.getAttribute("loggedInUser");
        if (loggedInUser == null) {
            return new ResponseDto("로그인이 필요합니다", HttpStatus.INTERNAL_SERVER_ERROR.value());
        }
        Long memberId = loggedInUser.getId();
        if (memberId == null) {
            return new ResponseDto("존재하지 않는 회원입니다", HttpStatus.INTERNAL_SERVER_ERROR.value());
        }
        return  pointService.incomePoint(request,memberId);
    }






}