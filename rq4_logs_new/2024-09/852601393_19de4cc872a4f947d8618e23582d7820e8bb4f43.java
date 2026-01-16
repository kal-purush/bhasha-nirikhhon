package com.boot.DTO;

import java.security.Timestamp;

import lombok.Data;

@Data
public class Movieinfotb_vDTO {
	private String movieno;
	private String movienm;
	private String movieen;
	private long ranking;
	private String directornm;
	private Timestamp releaseday;
	private Timestamp endday;
	private String salesAcc;
	private long audiAcc;
	private int times;
	private String intro;
	private String status;
	private String typeNm;
	private String genre;
	private String actor;
	private String showType;
	private long moviestatus;
	private String ratingno;
	private String moviebackimg;
	private String moviepostimg;
	private String movieyoutube;
	private String steelcut;
}