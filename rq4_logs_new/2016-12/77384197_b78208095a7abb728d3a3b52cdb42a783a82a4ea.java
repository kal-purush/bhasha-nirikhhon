package com.nhnent.basecamp.model;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;

import lombok.Data;

@Data
public class Board {
	private int boardId;
	private String boardEmail;
	private String boardPassword;
	private String boardContent;
	private LocalDateTime writeDateTime;
	private LocalDateTime editDateTime;
	
	public void setWriteDateTime(Date writeDateTime) {
		Instant instant = writeDateTime.toInstant();
		this.writeDateTime = instant.atZone(ZoneId.systemDefault()).toLocalDateTime();
	}
	
	public void setEditDateTime(Date editDateTime) {
		Instant instant = editDateTime.toInstant();
		this.editDateTime = instant.atZone(ZoneId.systemDefault()).toLocalDateTime();
	}
}