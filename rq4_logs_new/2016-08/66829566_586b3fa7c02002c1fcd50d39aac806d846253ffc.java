package com.rensframework.entity;

import java.util.Date;

import lombok.Getter;
import lombok.Setter;

import org.springframework.data.mongodb.core.mapping.Document;

@Document
@Getter@Setter
public class Message {
	
	private String id;
	/**
	 * 发送者
	 */
	private String fromId;
	
	private String fromName;
	
	/**
	 * 接受者
	 */
	private String toId;
	private String toName;
	
	private String content;
	
	private Date sendTime;

}