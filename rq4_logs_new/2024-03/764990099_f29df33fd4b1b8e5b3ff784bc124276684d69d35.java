package com.customs.network.fdapn.model;
import jakarta.persistence.*;
import lombok.Data;
import java.util.Date;

@Data
public class DailyAudit {

    private long id;

    @Column(name="userId")
    private String userId;

    @Column(name="date")
    private Date date;

    @Column(name="Success")
    private long successCount;

    @Column(name="Failed")
    private long failedCount;

    @Column(name="pending")
    private long pendingCount;

    @Column(name="cbpDown")
    private long cbpDownCount;

    @Column(name="Total_Transactions")
    private long totalTransactions;
}