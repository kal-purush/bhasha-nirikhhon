package com.hopper.model.availability;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
//@JsonInclude(JsonInclude.Include.NON_NULL)
public class CancelPolicies
{
    @JsonProperty("currency")
    private String currency;

    @JsonProperty("start")
    private String start;

    @JsonProperty("end")
    private String end;

    @JsonProperty("amount")
    private Double amount;

    @JsonProperty("nights")
    private int nights;

    @JsonProperty("percent")
    private Float percentage;

    public CancelPolicies(){}

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public void setStart(String start) {
        this.start = start;
    }

    public void setEnd(String end) {
        this.end = end;
    }

    public void setAmount(Double amount) {
        this.amount = amount;
    }

    public void setNights(int nights) {
        this.nights = nights;
    }

    public void setPercentage(Float percentage) {
        this.percentage = percentage;
    }

    public String getCurrency()
    {
        return currency;
    }

    public String getStart()
    {
        return start;
    }

    public String getEnd()
    {
        return end;
    }

    public Double getAmount()
    {
        return amount;
    }

    public int getNights()
    {
        return nights;
    }

    public Float getPercentage()
    {
        return percentage;
    }

    @Override
    public String toString() {
        return "CancelPolicies{" +
                "currency='" + currency + '\'' +
                ", start='" + start + '\'' +
                ", end='" + end + '\'' +
                ", amount=" + amount +
                ", nights=" + nights +
                ", percentage=" + percentage +
                '}';
    }
}