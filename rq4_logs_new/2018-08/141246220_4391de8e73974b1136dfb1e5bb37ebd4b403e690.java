package com.spstes.model;

import java.math.BigInteger;

public class ExamineeInfo {
	private BigInteger exa_stuID;
	private BigInteger tex_exaID;
	private int info_ID;
	private BigInteger exa_majorID;
	private BigInteger exa_num;
	private String exaID;
	private int pass;

	public ExamineeInfo() {
		super();
	}

	public BigInteger getExa_stuID() {
		return exa_stuID;
	}

	public void setExa_stuID(BigInteger exa_stuID) {
		this.exa_stuID = exa_stuID;
	}

	public BigInteger getTex_exaID() {
		return tex_exaID;
	}

	public void setTex_exaID(BigInteger tex_exaID) {
		this.tex_exaID = tex_exaID;
	}

	public int getInfo_ID() {
		return info_ID;
	}

	public void setInfo_ID(int info_ID) {
		this.info_ID = info_ID;
	}

	public BigInteger getExa_majorID() {
		return exa_majorID;
	}

	public void setExa_majorID(BigInteger exa_majorID) {
		this.exa_majorID = exa_majorID;
	}

	public BigInteger getExa_num() {
		return exa_num;
	}

	public void setExa_num(BigInteger exa_num) {
		this.exa_num = exa_num;
	}

	public String getExaID() {
		return exaID;
	}

	public void setExaID(String exaID) {
		this.exaID = exaID;
	}

	public int getPass() {
		return pass;
	}

	public void setPass(int pass) {
		this.pass = pass;
	}

}