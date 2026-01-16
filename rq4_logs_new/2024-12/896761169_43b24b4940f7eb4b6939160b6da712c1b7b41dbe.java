package com.springpro.hospital.ApiResponse;

public class TotalCostResponse {
    private double totalcost;

    public TotalCostResponse(double totalcost) {
        this.totalcost = totalcost;
    }

    public double getTotalcost() {
        return totalcost;
    }

    public void setTotalcost(double totalcost) {
        this.totalcost = totalcost;
    }
}