package com.service.spring.vo;



import java.util.Date;
import java.util.List;


import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;


@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class Contents { 
	private int contentId;  
	private int contentSum;   //모여진 금액
	private String contentName;
	private int target;    // 목표금액
	private String category;
	private Date startDay;
	private Date endDay;
	private int likes;
	private boolean isAccepted;
	
	private List<Image> images;
	private Support support;
	private Reviews review;
	private FundingContents fundingContents;
	private SupportContents supportContents;
	private List<Gift> gifts;
	
	
	
}