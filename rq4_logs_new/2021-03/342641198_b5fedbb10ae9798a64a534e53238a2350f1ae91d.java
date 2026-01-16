package com.banking.bankservice.model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.ManyToOne;
import lombok.Data;
import lombok.ToString;

@Entity(name = "account")
@Data
public class Account {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    @Column(name = "account_number", unique = true)
    private String accountNumber;
    @Enumerated(EnumType.STRING)
    @Column(name = "currency_type")
    private Currency currencyType;
    private Double balance;
    @Column(name = "active")
    private boolean isActive;
    @ManyToOne
    @ToString.Exclude
    private User user;
}