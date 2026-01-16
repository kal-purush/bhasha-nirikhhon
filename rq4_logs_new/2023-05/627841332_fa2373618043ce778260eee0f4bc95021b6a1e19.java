package com.island.app.seminar.vo;

import org.springframework.web.multipart.MultipartFile;

import lombok.Data;

@Data
public class SeminarVo {
	private String no;
	private String writerNo;
	private String bankNo;
	private String bankName;
	private String likeCount;
	private String name;
	private String intro;
	private String exintro;
	private String place; //도로명 주소
	private String detailAddress; //상세주소
	private String maxCapacity;
	private String currentCapacity;
	private String startDay; //세미나 일시
	private String seminarTime; //세미나 시작~종료시간
	private String sHour; 
	private String sMinute;
	private String fHour; 
	private String fMinute;
	private String closeDay; //세미나 모집마감 일시
	private String closeTime; //세미나 모집마감 시간
	private String cHour;
	private String cMinute;
	private String enrollDate;
	private String modifiedDate;
	private String hit;
	private String payYn;
	private String expense;
	private String acountnum;
	private String blockYn;
	private String reportYn;
	private String delYn;
	private String seminarThumbnail; //세미나 썸네일 사진파일
	
	
	public void setSeminarTime(String sHour, String sMinute, String fHour, String fMinute) {
		this.seminarTime =" " +sHour + ":" + sMinute + " ~ " + fHour + ":" +fMinute ;
	}
	
	public void setCloseTime(String cHour, String cMinute) {
		this.closeTime = " " + cHour + ":" + cMinute;
	}
}