package com.misonamoo.niaportal.domain;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Date;

@Getter
@Setter
@ToString
public class DwBase extends BasePaging{
    private int dwNo;
    private long userNo;
    private int fileNo;
    private String sportsGbCode;
    private String fileUrl;
    private String fileName;
    private Date regDate;
}