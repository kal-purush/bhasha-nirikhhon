package com.fpr.financialProduct;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Data
public class FinancialProductResponse {
    private Result result;

    // Getter와 Setter 메서드
}

@JsonInclude(JsonInclude.Include.NON_NULL)
@Data
class Result {

    @JsonProperty("total_count")
    private int totalCount;

    @JsonProperty("max_page_no")
    private int maxPageNo;

    @JsonProperty("now_page_no")
    private int nowPageNo;

    @JsonProperty("baseList")
    private List<FinancialProduct> baseList;

    @JsonProperty("optionList")
    private List<FinancialProductOption> optionList;

    // Getter와 Setter 메서드
}

@JsonInclude(JsonInclude.Include.NON_NULL)
@Data
class FinancialProduct {
    @JsonProperty("fin_co_no")
    private String financialCompanyNumber;

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

    // Getter와 Setter 메서드
}

@JsonInclude(JsonInclude.Include.NON_NULL)
@Data
class FinancialProductOption {
    @JsonProperty("fin_prdt_cd")
    private String financialProductCode;

    @JsonProperty("intr_rate_type_nm")
    private String interestRateTypeName;

    @JsonProperty("save_trm")
    private String saveTerm;

    @JsonProperty("intr_rate")
    private int interestRate;

    @JsonProperty("intr_rate2")
    private int interestRate2;

    // Getter와 Setter 메서드
}