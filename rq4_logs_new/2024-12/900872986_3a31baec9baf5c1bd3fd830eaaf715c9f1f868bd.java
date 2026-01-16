package com.smart_cities.city.dto;

public class AggregatedConsumptionByProvider {
    private Long providerId;
    private Long totalConsumption;
    private Double averageConsumption;

    public AggregatedConsumptionByProvider(Long providerId, Long totalConsumption, Double averageConsumption) {
        this.providerId = providerId;
        this.totalConsumption = totalConsumption;
        this.averageConsumption = averageConsumption;
    }

    public Long getProviderId() {
        return providerId;
    }

    public void setProviderId(Long providerId) {
        this.providerId = providerId;
    }

    public Long getTotalConsumption() {
        return totalConsumption;
    }

    public void setTotalConsumption(Long totalConsumption) {
        this.totalConsumption = totalConsumption;
    }

    public Double getAverageConsumption() {
        return averageConsumption;
    }

    public void setAverageConsumption(Double averageConsumption) {
        this.averageConsumption = averageConsumption;
    }
}