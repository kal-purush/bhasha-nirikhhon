package com.api.knowknowgram.payload.response;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
public class LogicDetailResponse {
    private Long id;

    @JsonProperty("rows_num")
    private Integer rowsNum;

    @JsonProperty("cols_num")
    private Integer colsNum; 

    @JsonProperty("row_hints")
    private String rowHints;

    @JsonProperty("col_hints")
    private String colHints;

    private String solution;    

    @JsonProperty("is_like")       
    private boolean isLike;

    public LogicDetailResponse(Long id, Integer rowsNum, Integer colsNum, String rowHints, String colHints, String solution, boolean isLike) {
        this.id = id;
        this.rowsNum = rowsNum;
        this.colsNum = colsNum;     
        this.rowHints = rowHints;                
        this.colHints = colHints;                
        this.solution = solution;                
        this.isLike = isLike;                
    }
}