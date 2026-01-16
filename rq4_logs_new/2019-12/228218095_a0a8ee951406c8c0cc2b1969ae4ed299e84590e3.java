package com.webvidhi.pubsub.modal;

import lombok.Getter;
import lombok.Setter;

@Getter @Setter
public class Status {
	
	public Status(int i, String descr) {
		this.code = i;
		this.descr = descr;
	}

	private int code;
	
	private String descr;

}