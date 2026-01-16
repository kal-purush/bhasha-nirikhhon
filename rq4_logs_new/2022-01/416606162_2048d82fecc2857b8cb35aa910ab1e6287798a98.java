package com.itrex.java.lab.report.controller;

import com.itrex.java.lab.report.entity.ReportDTO;
import com.itrex.java.lab.report.service.ReporterService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;

@RestController
@RequestMapping("/report/customers/countContracts/sumContracts/orderByContractCountAndSum")
@RequiredArgsConstructor
public class ReporterController {

    @Autowired
    private ReporterService service;

    @GetMapping
    @PreAuthorize("hasAuthority('report:read')")
    public ReportDTO find(@RequestParam(name = "firstStartContractDate") String firstStartContractDate,
                                @RequestParam(name = "lastStartContractDate") String lastStartContractDate,
                                @RequestParam(name = "startWithContractCount", defaultValue = "2147483647") int startWithContractCount,
                                @RequestParam(name = "size", defaultValue = "20") int size) {

        return service.findAllCustomer(LocalDate.parse(firstStartContractDate),
                LocalDate.parse(lastStartContractDate), startWithContractCount, size);
    }
}