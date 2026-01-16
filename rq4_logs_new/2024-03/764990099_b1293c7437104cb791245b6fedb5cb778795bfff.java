package com.customs.network.fdapn.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;

import java.time.LocalDate;

@Entity
@Table(name = "weekly_transaction_counts")
@Data
public class WeeklyTransactionCount {
    @Id
    private Long id;
    private LocalDate startDate;
    private LocalDate endDate;
    private Long userId;
    private Long successCount;
    private Long rejectCount;
    private Long pendingCount;
    private Long cbpDownCount;
    private Long totalCount;
}