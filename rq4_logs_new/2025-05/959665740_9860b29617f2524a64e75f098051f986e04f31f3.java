package com.example.backend.dtos;

import com.example.backend.models.Payment;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.LocalDate;

@Data
@AllArgsConstructor
public class FeePaymentDTO {
    private Long id;
    private Integer quantity;
    private PaymentDTO.ResPayment resident;
    private Integer amountPaid;
    @JsonProperty("status")
    private String status;
    @JsonProperty("date_paid")
    private LocalDate datePaid;

    public static FeePaymentDTO fromEntity(Payment payment) {
        return new FeePaymentDTO(
                payment.getId(),
                payment.getQuantity(),
                new PaymentDTO.ResPayment(payment.getResident()),
                payment.getAmountPaid(),
                payment.getStatus(),
                payment.getDatePaid()
        );
    }
}