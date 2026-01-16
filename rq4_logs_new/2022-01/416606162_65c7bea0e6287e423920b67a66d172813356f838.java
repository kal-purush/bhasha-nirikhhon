package com.itrex.java.lab.entity;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class CustomerReportPageParameter {

    private final String firstStartContractDate;
    private final String lastStartContractDate;
    private final int startWithContractCount;
    private final int size;
}