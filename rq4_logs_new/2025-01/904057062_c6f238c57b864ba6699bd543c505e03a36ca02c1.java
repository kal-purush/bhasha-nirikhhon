package com.eidiko.supermarket_action_service.dto;

import com.eidiko.supermarket_action_service.model.Stocks;

import java.util.List;

public class SaleRequest {

    private int employeeId;
    private List<Stocks> stocks;

    public SaleRequest() {
    }

    public SaleRequest(int employeeId, List<Stocks> stocks) {
        this.employeeId = employeeId;
        this.stocks = stocks;
    }

    public int getEmployeeId() {
        return employeeId;
    }

    public void setEmployeeId(int employeeId) {
        this.employeeId = employeeId;
    }

    public List<Stocks> getStocks() {
        return stocks;
    }

    public void setStocks(List<Stocks> stocks) {
        this.stocks = stocks;
    }

    @Override
    public String toString() {
        return "SalesDto{" +
                "employeeId=" + employeeId +
                ", stocks=" + stocks +
                '}';
    }
}