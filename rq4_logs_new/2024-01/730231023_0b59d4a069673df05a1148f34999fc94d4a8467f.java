package com.walkbuddies.backend.weather.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.walkbuddies.backend.common.response.SingleResponse;
import com.walkbuddies.backend.weather.service.WeatherShortService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;


@RestController
@RequiredArgsConstructor
public class WeatherShortController {

    private final WeatherShortService weatherShortService;

    @PostMapping("/weather/short/update")
    public SingleResponse weatherShortUpdate(@RequestParam Double x, @RequestParam Double y) throws JsonProcessingException {

        weatherShortService.update(x, y);
        SingleResponse singleResponse = new SingleResponse(HttpStatus.OK.value(), "단기예보 정보 업데이트에 성공하였습니다.", null);
        return singleResponse;
    }

}