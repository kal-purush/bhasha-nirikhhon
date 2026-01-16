package com.fpr.financialProduct.DTO;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@Data
public class FinancialProduct {
    @JsonProperty("fin_prdt_cd")
    private String financialProductCode;

    @JsonProperty("kor_co_nm")
    private String koreanCompanyName;

    @JsonProperty("fin_prdt_nm")
    private String financialProductName;

    @JsonProperty("join_way")
    private String joinWay;

    @JsonProperty("mtrt_int")
    private String maturityInterest;

    @JsonProperty("spcl_cnd")
    private String specialCondition;

    @JsonProperty("join_member")
    private String joinMember;

    @JsonProperty("etc_note")
    private String etcNote;

    @JsonProperty("max_limit")
    private int maxLimit;

    @JsonProperty("dcls_strt_day")
    private String declarationStartDate;

    @JsonProperty("fin_type_nm")
    private String financialTypeName;
}