package com.example.lifeonhana.dto.response;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import com.fasterxml.jackson.annotation.JsonProperty;

public record MyDataResponseDTO(
    @JsonProperty("pensionStart") String pensionStart,
    @JsonProperty("totalAsset") BigDecimal totalAsset,
    @JsonProperty("netAsset") BigDecimal netAsset,
    @JsonProperty("depositAmount") BigDecimal depositAmount,
    @JsonProperty("depositPercentage") Integer depositPercentage,
    @JsonProperty("savingsAmount") BigDecimal savingsAmount,
    @JsonProperty("savingsPercentage") Integer savingsPercentage,
    @JsonProperty("loanAmount") BigDecimal loanAmount,
    @JsonProperty("loanPercentage") Integer loanPercentage,
    @JsonProperty("stockAmount") BigDecimal stockAmount,
    @JsonProperty("stockPercentage") Integer stockPercentage,
    @JsonProperty("realEstateAmount") BigDecimal realEstateAmount,
    @JsonProperty("realEstatePercentage") Integer realEstatePercentage,
    @JsonProperty("lastUpdatedAt") LocalDateTime lastUpdatedAt,
    @JsonProperty("salaryAccount") SalaryAccountDTO salaryAccount,
    @JsonProperty("monthlyFixedExpense") BigDecimal monthlyFixedExpense
) {
    public record SalaryAccountDTO(
        @JsonProperty("accountNumber") String accountNumber,
        @JsonProperty("balance") BigDecimal balance,
        @JsonProperty("bank") String bank
    ) {}
} 