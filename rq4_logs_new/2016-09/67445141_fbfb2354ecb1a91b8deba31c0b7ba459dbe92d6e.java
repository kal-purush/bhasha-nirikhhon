package com.Polodz.controller;

import java.io.Serializable;

public class AuditRaport implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5823829955504180090L;
	
	private final String epiloque= "Full incomin: \n";
	private Integer numberOfFullAudience=0;
	
	private String errorBuffor="Warning for: \n";
	
	public void addToAudience(Integer extension) {
		this.numberOfFullAudience+=extension;
	}
	
	public void addToErrorBuffor(String extension) {
		this.errorBuffor += extension;
	}
	
	public Integer getNumberOfFullAudience() {
		return numberOfFullAudience;
	}

	private String getErrorBuffor() {
		return errorBuffor;
	}
	
	public String getRaportText() {
		String text = this.epiloque+this.getNumberOfFullAudience() +"\n";
		if ( errorBuffor.length()!="Warning for: \n".length()) 
			text+=this.errorBuffor+" are corrupted. Please restart application. If problem would occure more times please conntact superadmin";
		return text;
	}

}