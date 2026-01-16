package com.fpr.financialProduct.entity;

import jakarta.persistence.*;
import lombok.Data;

import java.util.List;

@Entity
@Data
@Table(name = "financial_product")
public class FinancialProductEntity {
    @Id
    @Column(name = "financial_product_code")
    private String financialProductCode;

    @Column(name = "korean_company_name")
    private String koreanCompanyName;

    @Column(name = "financial_product_name")
    private String financialProductName;

    @Column(name = "join_way")
    private String joinWay;

    @Column(name = "maturity_interest")
    private String maturityInterest;

    @Column(name = "special_condition")
    private String specialCondition;

    @Column(name = "join_member")
    private String joinMember;

    @Column(name = "etc_note")
    private String etcNote;

    @Column(name = "max_limit")
    private int maxLimit;

    @Column(name = "declaration_start_date")
    private String declarationStartDate;

    @Column(name = "financial_type_name")
    private String financialTypeName;

    @OneToMany(mappedBy = "financialProduct", cascade = CascadeType.ALL)
    private List<FinancialProductOptionEntity> options;
}