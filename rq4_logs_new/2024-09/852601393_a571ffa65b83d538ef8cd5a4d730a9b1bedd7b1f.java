package com.boot.DTO;

import java.security.Timestamp;
import java.sql.Date;

import lombok.Data;

@Data
public class Reserdtltb {
	private String uuid;
	private String reservenum;
	private int reserveno;
	private int areano;
	private int theaterno;
	private int movieno;
	private Date viewday;
	private int tmember;
	private int tprice;
	private Timestamp adate;
	private Timestamp mdate;
	private String fldaa;
	private String fldab;
	private String fldac;
	private String fldad;
	private int fldna;
	private int fldnb;
	private int fldnc;
	private int fldnd;
}