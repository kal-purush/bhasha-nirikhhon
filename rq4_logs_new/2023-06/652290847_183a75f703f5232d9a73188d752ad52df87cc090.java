package com.roller.doc.api.request;

import java.util.List;

import lombok.Data;

@Data
public class DrugMyReq {
	private Long userId;
	private String drugMyTitle;
	private String drugMyMemo;
	private List<Long> drugId;
}