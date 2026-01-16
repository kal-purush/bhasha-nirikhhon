package com.petid.api.hospital;

import com.petid.domain.hospital.model.HospitalOrderSummaryDTO;
import com.petid.domain.hospital.service.HospitalOrderService;
import com.petid.domain.hospital.type.OrderStatus;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequiredArgsConstructor
@RequestMapping("/v1/manage/hospital/order")
public class HospitalOrderManagerController {

    private final HospitalOrderService hospitalOrderService;

    @GetMapping
    public List<HospitalOrderSummaryDTO> findAllOrder(@RequestParam("status") OrderStatus status) {
        return hospitalOrderService.getAllOrders(status);

    }
}