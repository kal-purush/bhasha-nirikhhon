package com.fpr.financialProduct.controller;

import com.fpr.financialProduct.service.FinancialProductOptionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/options")
public class FinancialProductOptionController {
    private final FinancialProductOptionService financialProductOptionService;
}