package com.island.app.seminar.pay.vo;

import lombok.Data;

@Data
public class SeminarPayVo {
	private String no;
	private String sNo;
	private String mNo;
	private String pwNo;
	private String psNo;
	private String pDate;
	private String pWay; //결제수단
	private String pState; //결제상태
}