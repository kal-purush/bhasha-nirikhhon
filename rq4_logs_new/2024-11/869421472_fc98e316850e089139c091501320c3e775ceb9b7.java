package com.cinema.orderservice.controller;

import com.cinema.orderservice.dto.response.ApiResponse;
import com.cinema.orderservice.dto.response.StatisticalFoodResponse;
import com.cinema.orderservice.service.StatisticalFoodService;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE , makeFinal = true)
public class StatisticalFoodController {

    StatisticalFoodService statisticalFoodService;
    @GetMapping("/statistical-food")
    public ApiResponse<StatisticalFoodResponse> statisticalFood() {
        return ApiResponse.<StatisticalFoodResponse>builder()
                .code(1000)
                .msg("success")
                .data(statisticalFoodService.statisticalFood())
                .build();
    }
}